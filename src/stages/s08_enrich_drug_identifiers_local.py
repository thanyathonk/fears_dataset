from __future__ import annotations

"""Stage S08 Local – Enrich drug identifiers using Direct NCBI API Key Connection.

Logic (v8):
1. RxNav API exact match on basename
2. RxNav API exact match on each ingredient
3. LocalCID offline fallback: basename/ingredients → CID-Synonym-filtered.db
   → canonical title from CID-Title → retry RxNav
4. Salt/suffix stripping: remove pharmaceutical noise words (SULFATE, TABLETS,
   TEVA, MG, …) from basename, deduplicate repeated words → retry RxNav
5. ChEMBL verified brand lookup: search ChEMBL for brand name, accept ONLY if
   the search term appears as an EXACT (case-insensitive) match in the molecule's
   cross_references or synonyms → pref_name → RxNav
6. RxNav approximate match: fuzzy-tolerant RxNav search for typos/truncations
   (e.g. FLUCANOZOLE → FLUCONAZOLE).  Accepts ONLY when:
   (a) approximate score ≥ 8.0,
   (b) string-similarity(input, matched) ≥ 0.70 (SequenceMatcher),
   (c) for single-word inputs: first letter must match (prevents F→S swaps).
   → lookup_hit = "approx:<matched_name>"
7. KEGG Drug lookup → INN → RxNav: for non-US drugs (Japanese, European brands)
   not present in RxNorm under their brand name.  Searches KEGG Drug database,
   extracts the INN from the entry's NAME field, then queries RxNav with the INN.
   → lookup_hit = "kegg:<kegg_id>:<inn>"
8. Fetch rxnorm_ingredients for any resolved RxCUI

After the join, ``ing_source`` is coalesced with RxNorm path labels when still null
(see ``_coalesce_ing_source_with_rxnorm_enrichment``). Primary ``faers`` / ``llm`` /
``bracket`` labels come from S07 / ``s07_openai_run.py``, not from this stage.

Rows with no ``rxcui`` and rows matching **suspicious** ``medicinal_product`` heuristics
are **removed** from ``{cohort}_drugs_enriched.parquet`` and written under
``quarantine/{cohort}_drugs_quarantine.parquet`` for later review.

Set ``S08_QUARANTINE_ONLY_UNMAPPED=1`` to send only unmapped (no ``rxcui``) rows to
quarantine and keep suspicious names in the main file.
"""

import asyncio
import os
import re
import sqlite3
import urllib.parse
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Dict, List, Optional

import aiohttp
import polars as pl
from loguru import logger
from tqdm import tqdm

from src.adapters.rxnorm import RxNormClient, IngredientInfo
from src.utils.io import PipelineContext, stage_output_path, write_manifest


def _coalesce_ing_source_with_rxnorm_enrichment(df: pl.DataFrame) -> pl.DataFrame:
    """Resolve ``ing_source`` when it is still null after S07 but S08 found an RxCUI.

    **Lineage**

    - ``ing_source`` values ``faers`` | ``llm`` | ``bracket`` are produced in the
      S07 LLM decomposition step (see ``scripts/s07_openai_run.py``): they describe
      where the *structured* ``ingredients`` list came from. The in-repo
      ``s07b_llm_clean`` stage does not emit this column unless merged from that
      script or a compatible export.
    - S08 does **not** replace those values; it only adds RxNorm fields
      (``rxcui``, ``lookup_hit``, ``rxnorm_ingredients``, …).

    When ``ing_source`` is null but mapping succeeded, we record how RxNorm was
    reached so provenance is not left blank: ``rxnav_basename``, ``rxnav_ingredients``,
    or ``rxnorm_enriched`` for approx / KEGG / CID / ChEMBL / suffix paths.
    """
    if "ing_source" not in df.columns:
        df = df.with_columns(pl.lit(None, dtype=pl.Utf8).alias("ing_source"))

    rxnorm_label = (
        pl.when(pl.col("rxcui").is_null())
        .then(pl.lit(None, dtype=pl.Utf8))
        .when(pl.col("lookup_hit") == "basename")
        .then(pl.lit("rxnav_basename"))
        .when(pl.col("lookup_hit") == "ingredients")
        .then(pl.lit("rxnav_ingredients"))
        .otherwise(pl.lit("rxnorm_enriched"))
    )

    return df.with_columns(pl.coalesce(pl.col("ing_source"), rxnorm_label).alias("ing_source"))


def _suspicious_medicinal_product_expr() -> pl.Expr:
    """Heuristic: FAERS placeholders / non-drug-ish labels — quarantine for manual review."""
    mp = (
        pl.col("medicinal_product")
        .cast(pl.Utf8, strict=False)
        .fill_null("")
        .str.to_uppercase()
    )
    patterns = (
        "(UNKNOWN)",
        "(FORMULATION UNKNOWN)",
        "(GENERIC) (UNKNOWN)",
        "CHINESE HERBAL",
        "HERBAL MEDICINES",
        "CARDIOVASCULAR DRUG UNKNOWN",
        "ALLERGY SHOT",
        "NOT A DRUG",
        "A POSITIVE PLAQ",
        "DIET AID",
        "5000 DIET AID",
        "DRUG UNKNOWN",
        "ANTIBIOTICS (ANTIBIOTICS)",
        "VACCINE UNKNOWN",
    )
    expr: pl.Expr = pl.lit(False)
    for p in patterns:
        expr = expr | mp.str.contains(p)
    return expr


def _split_main_and_quarantine(
    enriched_df: pl.DataFrame,
    *,
    quarantine_suspicious: bool,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Return (main_enriched, quarantine) with ``s08_quarantine_reason`` on quarantine rows."""
    enriched_df = enriched_df.with_columns(_suspicious_medicinal_product_expr().alias("_s08_suspicious"))
    use_susp = pl.lit(quarantine_suspicious)
    reason = (
        pl.when(pl.col("rxcui").is_null() & pl.col("_s08_suspicious"))
        .then(pl.lit("no_rxcui_and_suspicious"))
        .when(pl.col("rxcui").is_null())
        .then(pl.lit("no_rxcui"))
        .when(pl.col("_s08_suspicious") & use_susp)
        .then(pl.lit("suspicious_name"))
        .otherwise(pl.lit("keep"))
    )
    enriched_df = enriched_df.with_columns(reason.alias("s08_quarantine_reason"))

    main_df = enriched_df.filter(pl.col("s08_quarantine_reason") == "keep").drop(
        "_s08_suspicious", "s08_quarantine_reason"
    )
    quarantine_df = enriched_df.filter(pl.col("s08_quarantine_reason") != "keep").drop("_s08_suspicious")
    return main_df, quarantine_df


# NCBI / PubChem (optional; higher rate limits when set — get from https://www.ncbi.nlm.nih.gov/account/settings/)
NCBI_API_KEY = os.environ.get("NCBI_API_KEY", "").strip()

# Rate limiting
RATE_LIMIT_SEMAPHORE = asyncio.Semaphore(5)   # RxNav
CHEMBL_SEMAPHORE = asyncio.Semaphore(5)        # ChEMBL EBI
KEGG_SEMAPHORE = asyncio.Semaphore(3)          # KEGG REST (be gentle)

# Local CID file paths (relative to project root)
_DATA_DIR = Path(__file__).parents[2] / "data"
CID_SYNONYM_DB = _DATA_DIR / "CID-Synonym-filtered.db"
CID_TITLE_FILE = _DATA_DIR / "CID-Title"


# ── Pharmaceutical noise words for Step-4 suffix stripping ───────────────────
# Words that commonly appear after a drug name but are NOT part of the name itself.
# Stripping these lets RxNav match "MORPHIN SULFATE" → "MORPHIN", etc.
_PHARMA_NOISE: frozenset = frozenset({
    # Salt / counter-ion forms
    # NOTE: CALCIUM, MAGNESIUM, ZINC, POTASSIUM are intentionally excluded —
    # they can be the primary drug name (e.g. "MAGNESIUM" or "CALCIUM CARBONATE").
    # SODIUM is kept because it almost never appears as a standalone drug.
    "HYDROCHLORIDE", "HYDROCHLORIC", "HCL", "SULFATE", "SULPHATE",
    "SODIUM", "ALUMINIUM", "ALUMINUM", "AMMONIUM",
    "ACETATE", "MONOHYDRATE", "DIHYDRATE", "HEMIHYDRATE", "TRIHYDRATE",
    "TARTRATE", "BITARTRATE", "FUMARATE", "MALEATE", "MESYLATE", "MESILATE",
    "BESYLATE", "PHOSPHATE", "DIPHOSPHATE", "NITRATE", "CHLORIDE", "BROMIDE",
    "HYDROBROMIDE", "CITRATE", "SUCCINATE", "GLUCONATE", "LACTATE", "TOSYLATE",
    "STEARATE", "VALERATE", "PALMITATE", "BASE", "ANHYDROUS", "ANHYDRATE",
    # NOTE: OXIDE and HYDROXIDE removed — they appear in real drug names
    # (ZINC OXIDE, MAGNESIUM HYDROXIDE, HYDROGEN PEROXIDE, NITRIC OXIDE).
    "PROPIONATE", "BUTYRATE", "CAPROATE", "DECANOATE",
    # Dosage forms
    "TABLET", "TABLETS", "TABLETTEN", "COMPRIMIDO", "COMPRIME", "COMPRIMES",
    "CAPSULE", "CAPSULES", "CAP", "CAPS", "CAPLET", "CAPLETS", "GELCAP",
    "CHEWABLE", "EFFERVESCENT", "DISPERSIBLE", "SOLUBLE", "DISSOLVABLE",
    "INJECTION", "INJ", "INJECTABLE", "INFUSION", "INTRAVENOUS",
    "SOLUTION", "SOL", "SUSPENSION", "SUSP", "EMULSION", "MIXTURE",
    "CREAM", "OINTMENT", "GEL", "DROPS", "SPRAY", "PATCH", "LOTION",
    "SYRUP", "ELIXIR", "POWDER", "GRANULES", "GRANULE", "SACHET",
    "FILM", "COATED", "ORAL", "TOPICAL", "INHALATION", "INHALER",
    "NASAL", "SUBLINGUAL", "RECTAL", "SUPPOSITORY",
    "OPHTHALMIC", "OPTHALMIC", "OPTIC", "EYE", "EAR", "NEBULIZER", "NEBS",
    "NEBULISERS", "VIALS", "VIAL", "AMPULE", "AMPOULE",
    # Release modifiers
    "EXTENDED", "RELEASE", "MODIFIED", "CONTROLLED", "DELAYED",
    "IMMEDIATE", "SUSTAINED", "PROLONGED", "SLOW",
    "ER", "XR", "SR", "CR", "LA", "XL", "RETARD", "DEPOT",
    # Qualifiers / regulatory suffixes
    "FORTE", "PLUS", "JUNIOR", "PEDIATRIC", "ADULT", "CHILDREN", "CHILDRENS",
    "USP", "NF", "BP", "EP", "IP", "EFG",
    "STERILE", "PURIFIED", "BUFFERED", "AQUEOUS", "CONCENTRATE", "DILUTED",
    # Strength units (standalone – e.g. "URSODIOL 300 MG")
    "MG", "MCG", "UG", "ML", "G", "IU", "MEQ", "MMOL", "MOL",
    # Route abbreviations
    "PO", "IV", "IM", "SC", "SQ", "SUBQ",
    # Common manufacturer / brand suffixes
    "ACTAVIS", "TEVA", "MYLAN", "SANDOZ", "AUROBINDO", "HERITAGE",
    "PFIZER", "BAYER", "NOVARTIS", "WATSON", "RATIOPHARM", "STADA",
    "HEXAL", "ZENTIVA", "ARROW", "GENERIQUES", "GENERIC", "GENERICS",
    "RENAUDIN", "KABI", "ASPEN", "ACCORD", "KRKA", "APOTEX",
    # Timing / frequency
    "DAILY", "TWICE", "ONCE", "WEEKLY", "MONTHLY", "DAYS", "HOURS",
    "REPEAT", "EVERY", "CYCLES",
    # Misc noise
    "NOT", "BRAND", "NAME", "UNKNOWN", "SPECIFIED", "UNSPECIFIED",
    "BLINDED", "DOUBLE", "SINGLE", "PLACEBO",
    # Route / site descriptors
    "INTRAMUSCULAR", "SUBCUTANEOUS", "INTRAVENOUS", "INTRATHECAL",
    "INTRAPERITONEAL", "INTRADERMAL", "INTRAVITREAL",
    # Formulation descriptions
    "GASTRO", "RESISTANT", "ENTERIC",
    "LIQUID", "LYOPHILIZED", "LYOPHILISED", "RECONSTITUTED",
    # French / German / Italian / Spanish pharmaceutical terms
    "BUVABLE", "BUVABLES",            # FR: drinkable
    "POUR",                            # FR preposition (pour perfusion = for infusion)
    "DILUER",                          # FR: to dilute
    "PERFUSION",                       # FR: infusion drip
    "ENROBE", "ENROBES",               # FR: sugar-coated
    "COMPRIMES", "COMPRIME",           # FR: tablets (also in main set but add variants)
    "GELULE", "GELULES",               # FR: capsules
    "SIROP", "SIROP",                  # FR: syrup
    "COMPRESSE",                       # IT/FR: tablet/compress
    "SAFT",                            # DE: syrup/juice
    "TABLETTE", "TABLETTEN",           # DE: tablet(s)
    "LOSUNG", "LOESUNGEN",             # DE: solution
    "TROPFEN",                         # DE: drops
    "SUPPOSITORIEN",                   # DE: suppositories
    "GOCCE",                           # IT: drops
    "COMPRIMIDO", "COMPRIMIDOS",       # ES/PT: tablet(s)
    "SOLUCION",                        # ES: solution
    "SOLUCION", "SOLUZIONE",           # ES/IT: solution
    "BUCODISPERSABLE",                 # ES: orodispersible
    "INFANTIL", "INFANTILE",           # ES/FR/IT: pediatric
    "PEDIATRIQUE", "PAEDIATRIC",       # FR/UK: pediatric
    # Vaccine / biologic qualifiers
    "TETRA", "PENTA", "HEXA",          # multi-component vaccine indicators
    "MONOVALENT", "BIVALENT", "TRIVALENT", "QUADRIVALENT",
    "LIVE", "ATTENUATED", "INACTIVATED",
    "ADJUVANTED",
    # Misc procedural/admin noise
    "SUPPLEMENTATION",
    # Common English prepositions used in drug name phrases (never the drug itself)
    "FOR", "OF", "WITH", "WITHOUT", "AND",
    "GARGLE",
    "WASH",
    "FLUSH",
    "PREP", "PREPARATION",
    "COMPOUND", "COMPOUNDED",
    "MIXED",
    "FORMULATION",
    # More manufacturer suffixes found in data
    "PHARMA", "PHARM", "PHARMAS",
    "LABS", "LAB", "LABORATORY", "LABORATORIES",
    "BIOSCIENCES", "BIOTECH", "BIOLOGICS",
    "PHARMACEUTICAL", "PHARMACEUTICALS",
    "HEALTHCARE",
    "LTD", "LLC", "INC", "GMBH", "CORP",
})


def _strip_pharma_noise(name: str) -> str:
    """Remove pharmaceutical noise words and deduplicate repeated words.

    Examples::

        _strip_pharma_noise("MORPHIN SULFATE")              → "MORPHIN"
        _strip_pharma_noise("URSODIOL CAP")                 → "URSODIOL"
        _strip_pharma_noise("MONTELUKAST CHEWABLE TABLETS") → "MONTELUKAST"
        _strip_pharma_noise("REMIFENTANIL REMIFENTANIL")    → "REMIFENTANIL"
        _strip_pharma_noise("RISPERIDONE ZYGENERICS")       → "RISPERIDONE ZYGENERICS"
        # (ZYGENERICS is not in the noise set, so it stays)
    """
    words = name.upper().split()
    cleaned = [w for w in words if w not in _PHARMA_NOISE]
    # Deduplicate while preserving order
    seen: set = set()
    deduped: List[str] = []
    for w in cleaned:
        if w not in seen:
            seen.add(w)
            deduped.append(w)
    return " ".join(deduped)


class LocalCIDLookup:
    """Offline drug-name → canonical-title lookup using local PubChem CID files.

    Call LocalCIDLookup.build(drug_names) once before the async enrichment loop.
    Then use lookup.get(name) for O(1) dict access per drug – no network needed.
    """

    def __init__(self, name_to_title: dict) -> None:
        self._map = name_to_title  # key: name.lower(), value: canonical title

    def get(self, name: str) -> Optional[str]:
        return self._map.get(name.lower())

    def __len__(self) -> int:
        return len(self._map)

    @classmethod
    def build(cls, drug_names: List[str]) -> "LocalCIDLookup":
        """Precompute mapping for all given drug names (batch SQL + stream CID-Title)."""
        if not CID_SYNONYM_DB.exists():
            logger.warning(f"[LocalCID] DB not found: {CID_SYNONYM_DB} – local fallback disabled")
            return cls({})

        names_lower = list(set(n.lower() for n in drug_names if n))  # dedupe
        logger.info(f"[LocalCID] Querying {len(names_lower):,} drug names from {CID_SYNONYM_DB.name} …")

        # Step 1: batch synonym → CID lookup, chunked to stay under SQLite's
        # SQLITE_MAX_VARIABLE_NUMBER limit (999 on older builds, 32766 on newer).
        CHUNK = 900  # conservative, works on all SQLite versions
        conn = sqlite3.connect(str(CID_SYNONYM_DB))
        conn.execute("PRAGMA cache_size=-65536")
        conn.execute("PRAGMA temp_store=MEMORY")

        rows: list = []
        for i in range(0, len(names_lower), CHUNK):
            chunk = names_lower[i : i + CHUNK]
            placeholders = ",".join("?" * len(chunk))
            rows.extend(
                conn.execute(
                    f"SELECT synonym_lower, cid FROM cid_synonyms"
                    f" WHERE synonym_lower IN ({placeholders})",
                    chunk,
                ).fetchall()
            )
        conn.close()
        logger.info(f"[LocalCID] SQL done: {len(rows):,} matches across {(len(names_lower)+CHUNK-1)//CHUNK} chunks")

        name_to_cid: dict = {row[0]: row[1] for row in rows}
        found_cids: set = set(name_to_cid.values())
        logger.info(
            f"[LocalCID] Synonym match: {len(name_to_cid):,}/{len(drug_names):,} drugs"
            f" → {len(found_cids):,} unique CIDs"
        )

        if not found_cids:
            return cls({})

        # Step 2: stream CID-Title file once to get canonical names for matched CIDs
        if not CID_TITLE_FILE.exists():
            logger.warning(f"[LocalCID] CID-Title not found: {CID_TITLE_FILE}")
            return cls({})

        logger.info(f"[LocalCID] Scanning {CID_TITLE_FILE.name} for {len(found_cids):,} CIDs …")
        cid_to_title: dict = {}
        with open(CID_TITLE_FILE, "r", encoding="utf-8", errors="ignore") as fh:
            for line in fh:
                parts = line.rstrip("\n").split("\t", 1)
                if len(parts) != 2:
                    continue
                try:
                    cid = int(parts[0])
                except ValueError:
                    continue
                if cid in found_cids:
                    cid_to_title[cid] = parts[1]
                    if len(cid_to_title) == len(found_cids):
                        break  # all CIDs found – stop early

        logger.info(f"[LocalCID] Title resolved: {len(cid_to_title):,}/{len(found_cids):,} CIDs")

        # Step 3: name_lower → title (only if title was found)
        name_to_title = {
            name: cid_to_title[cid]
            for name, cid in name_to_cid.items()
            if cid in cid_to_title
        }
        logger.info(f"[LocalCID] Final lookup table: {len(name_to_title):,} drugs with canonical titles")
        return cls(name_to_title)


@dataclass
class EnrichmentResult:
    """Result of drug identifier enrichment."""
    rxcui: Optional[str] = None
    rxcui_tty: Optional[str] = None
    rxnorm_ingredients: List[str] = None
    ingredient_count: int = 0
    source: str = "not_found"
    # Name / method used to find RxCUI:
    #   "basename"            – basename matched RxNav exactly (Step 1)
    #   "ingredients"         – an ingredient matched RxNav exactly (Step 2)
    #   "<canonical title>"   – local CID canonical name matched RxNav (Step 3)
    #   "suffix_strip:<x>"    – stripped name <x> matched RxNav (Step 4)
    #   "<pref_name>"         – ChEMBL brand→generic pref_name matched RxNav (Step 5)
    #   "approx:<matched>"    – RxNav approximate match (typo/truncation) (Step 6)
    #   "kegg:<id>:<inn>"     – KEGG Drug → INN → RxNav (Step 7)
    #   "not_found"           – nothing matched
    lookup_hit: str = "not_found"

    def __post_init__(self):
        if self.rxnorm_ingredients is None:
            self.rxnorm_ingredients = []


# Global cache type for enrichment results (key = basename)
EnrichmentCache = Dict[str, EnrichmentResult]


async def _enrich_names_direct(
    ctx: PipelineContext,
    basename_to_ingredients: Dict[str, Optional[str]],
    cache: EnrichmentCache,
    *,
    label: Optional[str] = None,
    local_cid: Optional[LocalCIDLookup] = None,
) -> EnrichmentCache:
    """
    Enrich drug names using RxNav API + local CID + suffix stripping + ChEMBL.

    Per drug (key = basename):
      Step 1 – RxNav(basename)               → lookup_hit = "basename"
      Step 2 – RxNav(each ingredient)        → lookup_hit = "ingredients"
      Step 3 – LocalCID → canonical → RxNav  → lookup_hit = <canonical title>
      Step 4 – strip pharma noise → RxNav    → lookup_hit = "suffix_strip:<x>"
      Step 5 – ChEMBL exact brand → pref_name → RxNav
                                               → lookup_hit = <pref_name>
      Step 6 – Fetch rxnorm_ingredients (only if RxCUI found)
    """

    rxnav_headers = {
        "User-Agent": "DrugPipeline/1.0 (mailto:tttccc4589@gmail.com)",
        "Accept": "application/json",
    }
    if NCBI_API_KEY:
        rxnav_headers["api-key"] = NCBI_API_KEY
    chembl_headers = {
        "User-Agent": "DrugPipeline/1.0 (mailto:tttccc4589@gmail.com)",
        "Accept": "application/json",
    }
    connector = aiohttp.TCPConnector(
        limit=20, limit_per_host=10, ttl_dns_cache=300,
        use_dns_cache=True, keepalive_timeout=60, enable_cleanup_closed=True,
    )
    timeout_rxnav = aiohttp.ClientTimeout(total=30, connect=10, sock_read=10)
    # ChEMBL EBI can be slow or congested; short sock_read caused frequent
    # "Timeout on reading data from socket" under load (see DEBUG logs).
    timeout_chembl = aiohttp.ClientTimeout(total=60, connect=20, sock_read=45)

    async def _rxnav(client: RxNormClient, name: str) -> Optional[str]:
        """RxNav exact match with one retry. Returns RxCUI or None."""
        for attempt in range(2):
            try:
                async with RATE_LIMIT_SEMAPHORE:
                    candidates = await client.lookup_exact(name)
                if candidates:
                    return candidates[0]
                if attempt == 0:
                    await asyncio.sleep(0.5)
            except Exception as e:
                if attempt == 0:
                    await asyncio.sleep(2)
                else:
                    logger.warning(f"[Local] RxNav error '{name}': {e}")
        return None

    async def _chembl_brand(chembl_sess: aiohttp.ClientSession, name: str) -> Optional[str]:
        """Search ChEMBL for a drug name and return pref_name ONLY when the
        search term appears as an EXACT (case-insensitive) match in the
        molecule's cross_references or molecule_synonyms.

        Strict exact matching prevents false positives from ChEMBL's full-text
        search (e.g., 'DAILY SMOKER' accidentally matching OLOPATADINE).
        """
        url = (
            "https://www.ebi.ac.uk/chembl/api/data/molecule/search.json"
            f"?q={urllib.parse.quote(name)}&limit=5"
        )
        name_lower = name.lower().strip()
        data = None
        for attempt in range(2):
            try:
                async with CHEMBL_SEMAPHORE:
                    async with chembl_sess.get(url) as resp:
                        if resp.status != 200:
                            return None
                        data = await resp.json()
                break
            except Exception as e:
                if attempt == 0:
                    await asyncio.sleep(1.5)
                    continue
                logger.debug(f"[ChEMBL] Error for '{name}' after retry: {e}")
                return None
        if data is None:
            return None

        try:
            for mol in data.get("molecules", []):
                xref_names = {
                    x.get("xref_name", "").lower()
                    for x in mol.get("cross_references", [])
                }
                syn_names = {
                    s.get("molecule_synonym", "").lower()
                    for s in mol.get("molecule_synonyms", [])
                }
                all_known = xref_names | syn_names

                # STRICT exact match only – no substring allowed
                if name_lower in all_known:
                    pref = mol.get("pref_name")
                    logger.debug(f"[ChEMBL] Exact match: '{name}' → '{pref}'")
                    return pref
        except Exception as e:
            logger.debug(f"[ChEMBL] Parse error for '{name}': {e}")
            return None

    kegg_headers = {
        "User-Agent": "DrugPipeline/1.0 (mailto:tttccc4589@gmail.com)",
        "Accept": "text/plain",
    }
    timeout_kegg = aiohttp.ClientTimeout(total=15, connect=8, sock_read=8)
    _kegg_name_re = re.compile(r"^NAME\s+(.+?)(?:\s*\([^)]*\))?;?\s*$", re.IGNORECASE)
    _kegg_cont_re = re.compile(r"^\s+(.+?)(?:\s*\([^)]*\))?;?\s*$")

    async with aiohttp.ClientSession(
        connector=connector, timeout=timeout_rxnav, headers=rxnav_headers
    ) as session, aiohttp.ClientSession(
        timeout=timeout_chembl, headers=chembl_headers
    ) as chembl_session, aiohttp.ClientSession(
        timeout=timeout_kegg, headers=kegg_headers
    ) as kegg_session:

        client = RxNormClient(ctx, session)
        results: EnrichmentCache = {}

        # ── Step-6 helper: RxNav approximate match ────────────────────────────
        async def _rxnav_approx(name: str,
                                min_score: float = 8.0,
                                min_sim: float = 0.70) -> tuple:
            """Fuzzy RxNav lookup.  Returns (rxcui, matched_name) or (None, None).

            Accepts only when:
            - approximate score >= min_score (default 8.0)
            - SequenceMatcher similarity >= min_sim (default 0.70)
            - single-word inputs: first letter of input == first letter of match
              (prevents e.g. FULCONAZOLE → SULCONAZOLE or SOLON → COLON)
            """
            url = "https://rxnav.nlm.nih.gov/REST/approximateTerm.json"
            async with RATE_LIMIT_SEMAPHORE:
                try:
                    async with session.get(
                        url,
                        params={"term": name, "maxEntries": "3", "option": "0"},
                        timeout=aiohttp.ClientTimeout(total=10),
                    ) as resp:
                        if resp.status != 200:
                            return None, None
                        data = await resp.json()
                except Exception as e:
                    logger.debug(f"[Approx] Error '{name}': {e}")
                    return None, None

            candidates = data.get("approximateGroup", {}).get("candidate", [])
            name_lower = name.lower().strip()
            words_in = name_lower.split()

            for cand in candidates:
                if not cand.get("name"):
                    continue
                score = float(cand.get("score", 0))
                if score < min_score:
                    continue
                matched = cand["name"]
                sim = SequenceMatcher(None, name_lower, matched.lower()).ratio()
                if sim < min_sim:
                    continue
                # Single-word guard: first character must match (case-insensitive)
                if len(words_in) == 1:
                    if name_lower[0] != matched.lower()[0]:
                        logger.debug(
                            f"[Approx] Rejected (first-letter mismatch) "
                            f"'{name}' → '{matched}'"
                        )
                        continue
                logger.debug(
                    f"[Approx] Accepted '{name}' → '{matched}' "
                    f"(score={score:.1f}, sim={sim:.2f})"
                )
                return cand["rxcui"], matched
            return None, None

        # ── Step-7 helper: KEGG Drug → INN ────────────────────────────────────
        async def _kegg_inn(name: str) -> tuple:
            """Search KEGG Drug for *name* and return (kegg_id, inn) or (None, None).

            Uses KEGG's REST search to find the drug, then parses the entry's
            NAME field.  The INN is the first name listed before a semicolon,
            e.g. "Foscarnet sodium (USP/INN); Foscavir (TN)" → "Foscarnet sodium".
            """
            search_url = (
                "https://rest.kegg.jp/find/drug/"
                + urllib.parse.quote(name, safe="")
            )
            async with KEGG_SEMAPHORE:
                try:
                    async with kegg_session.get(search_url) as resp:
                        if resp.status != 200:
                            return None, None
                        text = await resp.text()
                    lines = [l.strip() for l in text.strip().split("\n") if l.strip()]
                    if not lines:
                        return None, None
                    kid = lines[0].split("\t", 1)[0].strip()

                    await asyncio.sleep(0.15)  # respect KEGG rate limit
                    async with kegg_session.get(
                        f"https://rest.kegg.jp/get/{kid}"
                    ) as resp2:
                        if resp2.status != 200:
                            return None, None
                        entry = await resp2.text()

                    # Parse NAME block (first occurrence, strip annotation like (INN))
                    inn: Optional[str] = None
                    in_name_block = False
                    for line in entry.split("\n"):
                        if line.startswith("NAME"):
                            in_name_block = True
                            raw = line.split(None, 1)[1].strip() if "\t" in line or " " in line[4:] else ""
                        elif in_name_block and line.startswith(" "):
                            raw = line.strip()
                        else:
                            if in_name_block:
                                break
                            continue

                        # Strip trailing semicolon and parenthetical annotations
                        raw = raw.rstrip(";").strip()
                        clean = re.sub(r"\s*\([^)]*\)", "", raw).strip().rstrip(";").strip()
                        if clean and len(clean) >= 3:
                            inn = clean
                            break

                    return (kid, inn) if inn else (None, None)

                except Exception as e:
                    logger.debug(f"[KEGG] Error '{name}': {e}")
                    return None, None

        async def handle(basename: str, ingredients_str: Optional[str]) -> None:
            try:
                rxcui: Optional[str] = None
                lookup_hit = "not_found"
                source = "not_found"

                # Parse semicolon-separated ingredients, drop blanks
                ing_list: List[str] = [
                    i.strip() for i in (ingredients_str or "").split(";") if i.strip()
                ]

                # ── Step 1: basename → RxNav ─────────────────────────────────
                if basename:
                    rxcui = await _rxnav(client, basename)
                    if rxcui:
                        lookup_hit = "basename"
                        source = "rxnav_basename"
                        logger.debug(f"[Local] ✅ S1-basename '{basename}' → {rxcui}")
                    await asyncio.sleep(0.05)

                # ── Step 2: each ingredient → RxNav ──────────────────────────
                if not rxcui and ing_list:
                    for ing in ing_list:
                        rxcui = await _rxnav(client, ing)
                        if rxcui:
                            lookup_hit = "ingredients"
                            source = "rxnav_ingredients"
                            logger.debug(f"[Local] ✅ S2-ingredient '{ing}' → {rxcui}")
                            break
                    await asyncio.sleep(0.05)

                # ── Step 3: LocalCID offline fallback ────────────────────────
                if not rxcui and local_cid is not None:
                    cid_candidates: List[str] = ([basename] if basename else []) + ing_list
                    for cid_name in cid_candidates:
                        canonical = local_cid.get(cid_name)
                        if canonical:
                            rxcui = await _rxnav(client, canonical)
                            if rxcui:
                                lookup_hit = canonical
                                source = "local_cid_fallback"
                                logger.info(
                                    f"[Local] 🎉 S3-LocalCID '{cid_name}'"
                                    f" → '{canonical}' → {rxcui}"
                                )
                                break
                    await asyncio.sleep(0.05)

                # ── Step 4: Pharma-noise stripping → RxNav ───────────────────
                # Remove salt forms, dosage forms, manufacturer names, etc.
                # then deduplicate repeated words, and retry RxNav.
                # E.g. "MORPHIN SULFATE" → "MORPHIN"; "URSODIOL CAP" → "URSODIOL"
                if not rxcui and basename:
                    stripped = _strip_pharma_noise(basename)
                    if stripped and stripped.upper() != basename.upper():
                        rxcui = await _rxnav(client, stripped)
                        if rxcui:
                            lookup_hit = f"suffix_strip:{stripped}"
                            source = "suffix_strip"
                            logger.debug(
                                f"[Local] ✅ S4-strip '{basename}' → '{stripped}' → {rxcui}"
                            )
                    await asyncio.sleep(0.05)

                # ── Step 5: ChEMBL verified brand → generic → RxNav ──────────
                # Search ChEMBL but ONLY accept if the search term is an EXACT
                # match in cross_references or molecule_synonyms.
                # Tries full basename first, then each individual word
                # (brand names are often single words like KIOVIG, PULMICORT).
                if not rxcui and basename:
                    # Candidates: full name + each word (if multi-word)
                    words = basename.split()
                    chembl_candidates: List[str] = [basename]
                    if len(words) > 1:
                        chembl_candidates.extend(w for w in words if len(w) >= 4)

                    for cand in chembl_candidates:
                        pref = await _chembl_brand(chembl_session, cand)
                        if pref:
                            rxcui = await _rxnav(client, pref)
                            if not rxcui:
                                # try first word of pref_name (e.g. "HUMAN IMMUNOGLOBULIN G" → "HUMAN")
                                first = pref.split()[0]
                                if first != pref:
                                    rxcui = await _rxnav(client, first)
                            if rxcui:
                                lookup_hit = pref
                                source = "chembl_brand"
                                logger.info(
                                    f"[Local] ✅ S5-ChEMBL '{cand}' → '{pref}' → {rxcui}"
                                )
                                break
                    await asyncio.sleep(0.05)

                # ── Step 6: RxNav approximate match (typos / truncations) ────
                # Handles cases like FLUCANOZOLE → FLUCONAZOLE, SIMETHCONE → Simethicone.
                # Uses strict filters to suppress false positives.
                if not rxcui and basename:
                    approx_rxcui, approx_name = await _rxnav_approx(basename)
                    if approx_rxcui:
                        rxcui = approx_rxcui
                        lookup_hit = f"approx:{approx_name}"
                        source = "approx_match"
                        logger.debug(
                            f"[Local] ✅ S6-approx '{basename}' → '{approx_name}' → {rxcui}"
                        )
                    await asyncio.sleep(0.05)

                # ── Step 7: KEGG Drug → INN → RxNav ──────────────────────────
                # For non-US drugs (Japanese, European brands) that are registered in
                # KEGG but not directly searchable in RxNorm under their brand name.
                # Extracts the INN from the KEGG entry and queries RxNav with it.
                # Minimum 5 chars required to avoid substring false positives in KEGG
                # (e.g. "AN" matches "Nadide", "G" matches "Oxygen").
                if not rxcui and basename and len(basename.strip()) >= 5:
                    kegg_id, inn = await _kegg_inn(basename)
                    if inn:
                        rxcui = await _rxnav(client, inn)
                        if not rxcui:
                            # try approximate on the KEGG INN too
                            approx_rxcui2, _ = await _rxnav_approx(inn)
                            if approx_rxcui2:
                                rxcui = approx_rxcui2
                        if rxcui:
                            lookup_hit = f"kegg:{kegg_id}:{inn}"
                            source = "kegg_inn"
                            logger.info(
                                f"[Local] ✅ S7-KEGG '{basename}' → "
                                f"INN='{inn}' ({kegg_id}) → {rxcui}"
                            )
                    await asyncio.sleep(0.05)

                # ── Step 8: fetch rxnorm_ingredients ─────────────────────────
                rxcui_tty: Optional[str] = None
                rxnorm_ingredients: List[str] = []

                if rxcui and rxcui.strip():
                    try:
                        async with RATE_LIMIT_SEMAPHORE:
                            ingredient_infos = await client.get_related_ingredients(rxcui)
                        rxnorm_ingredients = sorted(set(i.name for i in ingredient_infos))
                        tty_set = set(i.tty for i in ingredient_infos)
                        rxcui_tty = "IN" if "IN" in tty_set else ("MIN" if "MIN" in tty_set else None)
                    except Exception as e:
                        logger.warning(f"[Local] rxnorm_ingredients error {rxcui}: {e}")
                        source = f"{source}+ingredients_error"

                results[basename] = EnrichmentResult(
                    rxcui=rxcui,
                    rxcui_tty=rxcui_tty,
                    rxnorm_ingredients=rxnorm_ingredients,
                    ingredient_count=len(rxnorm_ingredients),
                    source=source,
                    lookup_hit=lookup_hit,
                )

            except Exception as exc:
                logger.warning(f"[Local] Enrichment failed '{basename}': {exc}", exc_info=True)
                results[basename] = EnrichmentResult(source="error", lookup_hit="not_found")

        results = {}
        tasks = [
            asyncio.create_task(handle(bn, ing))
            for bn, ing in basename_to_ingredients.items()
        ]
        desc = f"S08 [{label}]" if label else "S08 enrichment"
        pbar = tqdm(
            total=len(tasks), desc=desc, unit="drugs", ncols=110, colour="green",
            bar_format="{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]",
            leave=True, miniters=1, smoothing=0.1,
        )
        completed = 0

        for fut in asyncio.as_completed(tasks):
            await fut
            completed += 1
            pbar.update(1)
            if completed % 100 == 0:
                ok = sum(1 for r in results.values() if r.rxcui)
                pbar.set_postfix({"found": f"{ok}/{completed}", "rate": f"{ok/completed*100:.1f}%"}, refresh=True)
                if completed % 1000 == 0:
                    ok = sum(1 for r in results.values() if r.rxcui)
                    logger.info(f"[Local] Progress {completed}/{len(tasks)}: {ok} found ({ok/completed*100:.1f}%)")

        pbar.close()
        ok = sum(1 for r in results.values() if r.rxcui)
        logger.info(f"[Local] Session done: {len(results)} processed, {ok} with RxCUI")
        for bn, res in [(b, r) for b, r in results.items() if r.rxcui][:5]:
            logger.info(f"  ✓ '{bn}' → {res.rxcui} via {res.lookup_hit}")

        cache.update(results)
        return cache


def run(ctx: PipelineContext) -> None:
    """Run Stage S08 Local: Drug identifier enrichment using Direct NCBI API."""

    source_dir = stage_output_path(ctx, "s07b_llm_clean")
    output_dir = stage_output_path(ctx, "s08_enrich_drug_identifiers")
    output_dir.mkdir(parents=True, exist_ok=True)

    manifest_payload: Dict[str, Dict[str, object]] = {}
    global_cache: EnrichmentCache = {}

    # Check for force re-run flag
    force_enrichment = os.environ.get("FORCE_ENRICHMENT") == "1"
    if force_enrichment:
        logger.info("[Local] FORCE mode: Clearing cache, will re-run all enrichment")
        global_cache.clear()

    # Process each cohort (support selective cohort via environment variable)
    target_cohort = os.environ.get("TARGET_COHORT")
    if target_cohort:
        cohorts_to_process = (target_cohort,)
        logger.info(f"[Local] Processing selected cohort: {target_cohort}")
    else:
        cohorts_to_process = ("pediatric", "adult")
        logger.info("[Local] Processing all cohorts: pediatric, adult")

    for cohort in cohorts_to_process:
        # Try all known naming conventions (most specific first)
        candidates = [
            source_dir / f"{cohort}_drugs_llm_cleaned_full_data.parquet",
            source_dir / f"{cohort}_drugs_clean_full_data.parquet",
            source_dir / f"{cohort}_drugs_llm_cleaned.parquet",
            source_dir / f"{cohort}_drugs_clean.parquet",
        ]
        source = next((p for p in candidates if p.exists()), None)

        if source is None:
            raise FileNotFoundError(
                f"Missing LLM-cleaned drug dataset for cohort '{cohort}'.\n"
                f"Searched:\n" + "\n".join(f"  - {p}" for p in candidates)
            )

        logger.info(f"[Local] Cohort {cohort}: using source file {source.name}")

        df = pl.read_parquet(source)
        logger.debug(f"[Local] Loaded DF shape: {df.shape}, columns: {list(df.columns)}")

        # Clean schema: drop legacy columns
        legacy_cols = ["__index_level_0__", "rxcui", "rxcui_tty", "ingredient_count"]
        for col in legacy_cols:
            if col in df.columns:
                df = df.drop(col)
                logger.debug(f"[Local] Dropped legacy column: {col}")

        # Determine lookup column: prefer 'basename' (LLM-cleaned), fall back to 'medicinal_product_llm_clean'
        if "basename" in df.columns:
            lookup_col = "basename"
        elif "medicinal_product_llm_clean" in df.columns:
            lookup_col = "medicinal_product_llm_clean"
        else:
            raise ValueError(
                f"[Local] Required column 'basename' or 'medicinal_product_llm_clean' not found in {source}. "
                f"Found columns: {list(df.columns)}"
            )
        logger.info(f"[Local] Using '{lookup_col}' as drug name lookup key")
        logger.debug(f"[Local] After cleaning: shape {df.shape}, columns: {list(df.columns)}")

        # Filter rows with valid drug names (keep medicinal_product raw intact)
        df = df.filter(pl.col(lookup_col).is_not_null())

        # Build list of names to enrich
        df_with_id = df.with_row_count("row_id")
        col_dtype = df_with_id.get_column(lookup_col).dtype

        if col_dtype == pl.List(pl.Utf8):
            names_exploded = (
                df_with_id
                .select("row_id", pl.col(lookup_col))
                .explode(lookup_col)
                .rename({lookup_col: "name"})
                .drop_nulls("name")
            )
        elif col_dtype == pl.Utf8:
            names_exploded = (
                df_with_id
                .select("row_id", pl.col(lookup_col).alias("name"))
                .drop_nulls("name")
            )
        else:
            raise TypeError(f"Unsupported dtype for {lookup_col}: {col_dtype}")

        # ── Build (basename → ingredients_str) mapping, sorted by frequency ──
        # Each unique basename maps to its first-seen ingredients string.
        # Frequency used to process popular drugs first (better cache reuse).
        has_ingredients_col = "ingredients" in df.columns

        bn_freq: Dict[str, int] = {}
        bn_to_ing: Dict[str, Optional[str]] = {}
        for row in df.iter_rows(named=True):
            bn = row.get("basename") or row.get("medicinal_product_llm_clean")
            if not bn:
                continue
            bn_freq[bn] = bn_freq.get(bn, 0) + 1

            # Normalise ingredients to semicolon-separated string.
            # New S07b files use List(String); older files used semicolon String.
            ing_val = row.get("ingredients") if has_ingredients_col else None
            if isinstance(ing_val, list):
                ing_val = ";".join(str(x) for x in ing_val if x) or None

            if bn not in bn_to_ing:
                bn_to_ing[bn] = ing_val
            elif bn_to_ing[bn] is None and ing_val:
                # Bug fix: first occurrence had null ingredients but a later row has them.
                # Update so Step 2 (ingredients → RxNav) can still fire for this basename.
                bn_to_ing[bn] = ing_val

        # Sort by frequency descending
        unique_names = sorted(bn_to_ing.keys(), key=lambda b: bn_freq.get(b, 0), reverse=True)
        logger.info(f"[Local] Cohort {cohort}: {len(unique_names):,} unique basenames to process")
        top5 = [(bn, bn_freq[bn]) for bn in unique_names[:5]]
        logger.info(f"[Local] Top 5 most frequent: {top5}")

        # Mark drugs with no searchable name at all (basename=None AND ingredients=None)
        missing = [bn for bn in unique_names if bn not in global_cache]

        SKIP_KEYWORDS = [
            "COLONEL", "BUTTOCKS", "MANTOUX", "CHOCOLA", "MA DOPARK",
            "OTHER ANALGESICS", "MULTIVITAMIN", "POLLENS",
        ]
        filtered_missing: Dict[str, Optional[str]] = {}
        skipped = 0
        for bn in missing:
            if any(kw in bn.upper() for kw in SKIP_KEYWORDS):
                global_cache[bn] = EnrichmentResult(source="skipped_non_drug", lookup_hit="not_found")
                skipped += 1
            else:
                filtered_missing[bn] = bn_to_ing.get(bn)

        logger.info(
            f"[Local] Cohort {cohort}: total={len(unique_names)}, cached={len(global_cache)}, "
            f"to_process={len(filtered_missing)}, skipped={skipped}"
        )

        if filtered_missing:
            # Build LocalCIDLookup with all possible names (basenames + ingredients)
            all_names_for_cid: List[str] = list(filtered_missing.keys())
            for ing_str in filtered_missing.values():
                if ing_str:
                    all_names_for_cid.extend(
                        i.strip() for i in ing_str.split(";") if i.strip()
                    )
            local_cid = LocalCIDLookup.build(all_names_for_cid)
            logger.info(f"[Local] LocalCID ready: {len(local_cid):,} names with canonical titles")

            result_cache = asyncio.run(_enrich_names_direct(
                ctx, filtered_missing, global_cache, label=cohort, local_cid=local_cid
            ))
            global_cache.update(result_cache)

            enriched_count = sum(1 for bn in filtered_missing if global_cache.get(bn) and global_cache[bn].rxcui)
            logger.info(f"[Local] Enrichment done: {enriched_count}/{len(filtered_missing)} found RxCUI")
        else:
            logger.info(f"[Local] Cohort {cohort}: all {len(unique_names):,} drugs already cached")

        # Debug: Final cache status
        total_in_cache = sum(1 for result in global_cache.values() if result and result.rxcui and result.rxcui.strip())
        logger.info(f"[Local] Cohort {cohort}: Final cache status - {len(global_cache)} total entries, {total_in_cache} with RxCUI")

        # Build mapping DataFrame from cache
        logger.debug(f"[Local] Building map DataFrame for {len(unique_names)} unique names")
        map_rows = []

        found_in_cache = 0
        for name in unique_names:
            result = global_cache.get(name)
            if result and result.rxcui:
                map_rows.append({
                    "name": name,
                    "rxcui": result.rxcui,
                    "rxcui_tty": result.rxcui_tty,
                    "rxnorm_ingredients": result.rxnorm_ingredients,
                    "ingredient_count": result.ingredient_count,
                    "lookup_hit": result.lookup_hit,
                })
                found_in_cache += 1
            else:
                map_rows.append({
                    "name": name,
                    "rxcui": None,
                    "rxcui_tty": None,
                    "rxnorm_ingredients": [],
                    "ingredient_count": 0,
                    "lookup_hit": "not_found",
                })

        logger.info(f"[Local] Cohort {cohort}: Map DataFrame created - {found_in_cache}/{len(unique_names)} drugs have RxCUI")

        # Create output DataFrame
        map_df = pl.DataFrame(map_rows)

        # Validate map_df has required columns
        required_map_cols = ["name", "rxcui", "rxcui_tty", "rxnorm_ingredients", "ingredient_count", "lookup_hit"]
        for col in required_map_cols:
            if col not in map_df.columns:
                raise ValueError(f"[Local] Required column '{col}' missing from map DataFrame")

        # Debug: Log column names before join
        logger.debug(f"[Local] Original DF columns: {list(df.columns)}")
        logger.debug(f"[Local] Map DF columns: {list(map_df.columns)}")

        # Join enrichment results back (medicinal_product raw stays untouched)
        enriched_df = df.join(
            map_df,
            left_on=lookup_col,
            right_on="name",
            how="left"
        )
        logger.debug(f"[Local] After join columns: {list(enriched_df.columns)}")

        # S07b has 'ingredients' (LLM string e.g. "FISH OIL;TOCOPHEROL").
        # map_df also adds 'ingredients' (RxNorm List[str]) → Polars renames it 'ingredients_right'.
        # Rename to 'rxnorm_ingredients' to keep both without conflict.
        if "ingredients_right" in enriched_df.columns:
            enriched_df = enriched_df.rename({"ingredients_right": "rxnorm_ingredients"})
            logger.debug("[Local] Renamed RxNorm ingredients to 'rxnorm_ingredients'")
        elif "ingredients" in enriched_df.columns and map_df.schema.get("ingredients") is not None:
            # No conflict (LLM had no ingredients col), just rename map_df's ingredients
            enriched_df = enriched_df.rename({"ingredients": "rxnorm_ingredients"})

        logger.debug(f"[Local] Final columns: {list(enriched_df.columns)}")

        enriched_df = _coalesce_ing_source_with_rxnorm_enrichment(enriched_df)

        quarantine_only_unmapped = os.environ.get("S08_QUARANTINE_ONLY_UNMAPPED", "").strip().lower() in (
            "1",
            "true",
            "yes",
            "y",
        )
        quarantine_suspicious = not quarantine_only_unmapped
        main_df, quarantine_df = _split_main_and_quarantine(
            enriched_df,
            quarantine_suspicious=quarantine_suspicious,
        )

        quarantine_dir = output_dir / "quarantine"
        quarantine_dir.mkdir(parents=True, exist_ok=True)
        quarantine_file = quarantine_dir / f"{cohort}_drugs_quarantine.parquet"

        output_file = output_dir / f"{cohort}_drugs_enriched.parquet"
        if main_df.height == 0:
            raise ValueError(
                f"[S08] {cohort}: no rows left after quarantine split — "
                "relax heuristics or set S08_QUARANTINE_ONLY_UNMAPPED=1."
            )

        main_df.write_parquet(output_file)
        logger.info(f"[Local] Saved {main_df.height:,} enriched rows (main) → {output_file}")

        if quarantine_df.height > 0:
            quarantine_df.write_parquet(quarantine_file)
            logger.warning(
                f"[Local] Quarantine {quarantine_df.height:,} rows → {quarantine_file} "
                f"(reasons: s08_quarantine_reason; suspicious_filter={'on' if quarantine_suspicious else 'off'})"
            )
        else:
            logger.info("[Local] Quarantine is empty (no rows dropped).")

        # ── Per-cohort statistics ─────────────────────────────────────────────
        total_drugs = len(unique_names)
        mapped_rows = sum(1 for r in global_cache.values() if r.rxcui is not None)
        rxnav_exact = sum(1 for r in global_cache.values() if r.source == "rxnav_basename")
        rxnav_ing = sum(1 for r in global_cache.values() if r.source == "rxnav_ingredients")
        local_cid_hit = sum(1 for r in global_cache.values() if r.source == "local_cid_fallback")
        suffix_strip_hit = sum(1 for r in global_cache.values() if r.source == "suffix_strip")
        chembl_hit = sum(1 for r in global_cache.values() if r.source == "chembl_brand")
        approx_hit = sum(1 for r in global_cache.values() if r.source == "approx_match")
        kegg_hit = sum(1 for r in global_cache.values() if r.source == "kegg_inn")
        not_found = total_drugs - mapped_rows
        coverage = mapped_rows / total_drugs * 100 if total_drugs > 0 else 0

        bar_filled = int(coverage / 5)
        bar = "█" * bar_filled + "░" * (20 - bar_filled)
        coverage_icon = "🌟" if coverage >= 90 else "✅" if coverage >= 80 else "⚠️ " if coverage >= 70 else "❌"

        logger.info("")
        logger.info("┌─────────────────────────────────────────────────────────┐")
        logger.info(f"│  [S08] {cohort.upper()} Cohort Summary" + " " * (33 - len(cohort)) + "│")
        logger.info("├─────────────────────────────────────────────────────────┤")
        logger.info(f"│  Total unique drugs      : {total_drugs:>10,}                   │")
        logger.info(f"│  RxNav (basename)        : {rxnav_exact:>10,}  ({rxnav_exact/total_drugs*100 if total_drugs else 0:5.1f}%)          │")
        logger.info(f"│  RxNav (ingredients)     : {rxnav_ing:>10,}  ({rxnav_ing/total_drugs*100 if total_drugs else 0:5.1f}%)          │")
        logger.info(f"│  LocalCID fallback       : {local_cid_hit:>10,}  ({local_cid_hit/total_drugs*100 if total_drugs else 0:5.1f}%)          │")
        logger.info(f"│  Suffix stripping        : {suffix_strip_hit:>10,}  ({suffix_strip_hit/total_drugs*100 if total_drugs else 0:5.1f}%)          │")
        logger.info(f"│  ChEMBL brand lookup     : {chembl_hit:>10,}  ({chembl_hit/total_drugs*100 if total_drugs else 0:5.1f}%)          │")
        logger.info(f"│  Approx match (typo)     : {approx_hit:>10,}  ({approx_hit/total_drugs*100 if total_drugs else 0:5.1f}%)          │")
        logger.info(f"│  KEGG Drug (non-US)      : {kegg_hit:>10,}  ({kegg_hit/total_drugs*100 if total_drugs else 0:5.1f}%)          │")
        logger.info(f"│  Total enriched          : {mapped_rows:>10,}  ({coverage:5.1f}%)          │")
        logger.info(f"│  Not found               : {not_found:>10,}  ({not_found/total_drugs*100 if total_drugs else 0:5.1f}%)          │")
        logger.info(f"│  Coverage  {coverage_icon} [{bar}] {coverage:.1f}%  │")
        logger.info("└─────────────────────────────────────────────────────────┘")
        logger.info("")

        if coverage >= 90:
            logger.success(f"[Local] Cohort {cohort}: 🌟 EXCELLENT ({coverage:.1f}%) – Production ready!")
        elif coverage >= 80:
            logger.success(f"[Local] Cohort {cohort}: ✅ GOOD ({coverage:.1f}%) – Ready for production!")
        elif coverage >= 70:
            logger.warning(f"[Local] Cohort {cohort}: ⚠️  MODERATE ({coverage:.1f}%) – Acceptable for research")
        else:
            logger.error(f"[Local] Cohort {cohort}: ❌ LOW ({coverage:.1f}%) – May need data quality improvements")

        manifest_payload[cohort] = {
            "total_drugs": total_drugs,
            "mapped_rows": mapped_rows,
            "coverage_pct": round(coverage, 2),
            "rxnav_basename": rxnav_exact,
            "rxnav_ingredients": rxnav_ing,
            "local_cid_fallback": local_cid_hit,
            "suffix_strip": suffix_strip_hit,
            "chembl_brand": chembl_hit,
            "approx_match": approx_hit,
            "kegg_inn": kegg_hit,
            "not_found": not_found,
            "main_output_rows": main_df.height,
            "quarantine_rows": quarantine_df.height,
            "quarantine_path": str(quarantine_file),
            "quarantine_suspicious_enabled": quarantine_suspicious,
        }

    # ── Overall summary ───────────────────────────────────────────────────────
    total_drugs_all = sum(s["total_drugs"] for s in manifest_payload.values())
    total_enriched_all = sum(s["mapped_rows"] for s in manifest_payload.values())
    total_not_found_all = sum(s["not_found"] for s in manifest_payload.values())
    total_rxnav_all = sum(s["rxnav_basename"] + s["rxnav_ingredients"] for s in manifest_payload.values())
    total_local_cid_all = sum(s["local_cid_fallback"] for s in manifest_payload.values())
    total_suffix_all = sum(s["suffix_strip"] for s in manifest_payload.values())
    total_chembl_all = sum(s["chembl_brand"] for s in manifest_payload.values())
    total_approx_all = sum(s["approx_match"] for s in manifest_payload.values())
    total_kegg_all = sum(s["kegg_inn"] for s in manifest_payload.values())
    overall_coverage = total_enriched_all / total_drugs_all * 100 if total_drugs_all > 0 else 0

    overall_bar = "█" * int(overall_coverage / 5) + "░" * (20 - int(overall_coverage / 5))
    overall_icon = "🌟" if overall_coverage >= 90 else "✅" if overall_coverage >= 80 else "⚠️ " if overall_coverage >= 70 else "❌"

    logger.info("")
    logger.info("╔═════════════════════════════════════════════════════════╗")
    logger.info("║           [S08] OVERALL ENRICHMENT SUMMARY             ║")
    logger.info("╠═════════════════════════════════════════════════════════╣")
    for cname, stats in manifest_payload.items():
        logger.info(
            f"║  {cname:10s}  total={stats['total_drugs']:>7,}  found={stats['mapped_rows']:>7,}"
            f"  miss={stats['not_found']:>7,}  ({stats['coverage_pct']:.1f}%) ║"
        )
    logger.info("╠═════════════════════════════════════════════════════════╣")
    logger.info(
        f"║  TOTAL       total={total_drugs_all:>7,}  found={total_enriched_all:>7,}"
        f"  miss={total_not_found_all:>7,}         ║"
    )
    logger.info("╠═════════════════════════════════════════════════════════╣")
    logger.info("║  Source breakdown                                       ║")
    logger.info(f"║    RxNav exact    : {total_rxnav_all:>7,}  ({total_rxnav_all/total_drugs_all*100 if total_drugs_all else 0:.1f}%)                       ║")
    logger.info(f"║    LocalCID       : {total_local_cid_all:>7,}  ({total_local_cid_all/total_drugs_all*100 if total_drugs_all else 0:.1f}%)                       ║")
    logger.info(f"║    Suffix strip   : {total_suffix_all:>7,}  ({total_suffix_all/total_drugs_all*100 if total_drugs_all else 0:.1f}%)                       ║")
    logger.info(f"║    ChEMBL brand   : {total_chembl_all:>7,}  ({total_chembl_all/total_drugs_all*100 if total_drugs_all else 0:.1f}%)                       ║")
    logger.info(f"║    Approx match   : {total_approx_all:>7,}  ({total_approx_all/total_drugs_all*100 if total_drugs_all else 0:.1f}%)                       ║")
    logger.info(f"║    KEGG non-US    : {total_kegg_all:>7,}  ({total_kegg_all/total_drugs_all*100 if total_drugs_all else 0:.1f}%)                       ║")
    logger.info(f"║    Not found      : {total_not_found_all:>7,}  ({total_not_found_all/total_drugs_all*100 if total_drugs_all else 0:.1f}%)                       ║")
    logger.info("╠═════════════════════════════════════════════════════════╣")
    logger.info(f"║  Coverage  {overall_icon} [{overall_bar}] {overall_coverage:.1f}%  ║")
    logger.info("╚═════════════════════════════════════════════════════════╝")
    logger.info("")

    write_manifest(
        ctx,
        "s08_enrich_drug_identifiers_local",
        {
            "stage": "s08_enrich_drug_identifiers_local",
            "version": "8.0",
            "connection_method": (
                "rxnav_api_key + local_cid_offline + suffix_strip"
                " + chembl_brand + approx_match + kegg_inn"
            ),
            "api_key_configured": bool(NCBI_API_KEY),
            "pubchem_api_calls": False,
            "overall_coverage_pct": round(overall_coverage, 2),
            "total_drugs_processed": total_drugs_all,
            "total_drugs_enriched": total_enriched_all,
            "cohorts": manifest_payload,
        }
    )

    if overall_coverage >= 80:
        logger.success(f"[Local] 🎉 PIPELINE SUCCESS: {overall_coverage:.1f}% overall coverage – Production ready!")
    else:
        logger.warning(f"[Local] ⚠️  PIPELINE COMPLETE: {overall_coverage:.1f}% overall coverage – Monitor and improve data quality")
