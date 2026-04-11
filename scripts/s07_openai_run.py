#!/usr/bin/env python3
"""
S07b LLM decomposition — OpenAI-compatible API version (vLLM endpoint).

Changes vs s07_ez_to_run.py:
  - Uses OpenAI client instead of local transformers
  - Batch checkpoint: saves per-batch parquet, merges at end
  - _parse_substance: uses active_substance_faers directly for ingredients
    (bypasses LLM ingredient extraction when ground truth is available)
  - pyarrow schema: ingredients/salt stored as list<string>, not string
  - _str_or_none: cleans "None"/"null" strings to proper null
"""

import glob
import json
import logging
import os
import re
import time
import warnings
from pathlib import Path

import src.settings  # noqa: F401 — loads repository .env before os.environ reads

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from openai import OpenAI
from tqdm import tqdm

# =========================
# Setup
# =========================
warnings.filterwarnings("ignore")
os.environ["TOKENIZERS_PARALLELISM"] = "false"

logging.basicConfig(
    format="%(asctime)s -%(levelname)s- %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# =========================
# Config — set via environment (no secrets in repo)
# =========================
_REPO_ROOT = Path(__file__).resolve().parents[1]

def _env_path(key: str, default: str) -> Path:
    raw = os.environ.get(key, default).strip()
    p = Path(raw)
    return p if p.is_absolute() else (_REPO_ROOT / p).resolve()

INPUT_PARQUET = _env_path(
    "S07_INPUT_PARQUET",
    "data/staging/s07_split_drug/pediatric_drugs_full_data.parquet",
)
BATCH_DIR = _env_path("S07_BATCH_DIR", "data/staging/s07b_llm_batches")
OUTPUT_PARQUET = _env_path(
    "S07_OUTPUT_PARQUET",
    "data/staging/s07b_llm_clean/pediatric_drugs_llm_cleaned_full_data.parquet",
)

API_BASE_URL = os.environ.get("S07_API_BASE_URL", "http://127.0.0.1:8000/v1").strip()
# OpenAI-compatible key (vLLM often accepts any non-empty string locally)
API_KEY = os.environ.get("S07_API_KEY", os.environ.get("OPENAI_API_KEY", "")).strip()
MODEL_NAME = os.environ.get("S07_MODEL_NAME", "Qwen/Qwen2.5-32B-Instruct").strip()

BATCH_SIZE = int(os.environ.get("S07_BATCH_SIZE", "20"))
SLEEP_SEC = float(os.environ.get("S07_SLEEP_SEC", "0.1"))

# =========================
# OpenAI client
# =========================
if not API_KEY:
    raise SystemExit(
        "Set S07_API_KEY or OPENAI_API_KEY (OpenAI-compatible endpoint).\n"
        "Example: export S07_API_BASE_URL=http://127.0.0.1:8000/v1"
    )

client = OpenAI(base_url=API_BASE_URL, api_key=API_KEY)

# =========================
# Load dataset
# =========================
logger.info("Loading dataset")
ds = pd.read_parquet(INPUT_PARQUET)
assert "medicinal_product" in ds.columns
logger.info(f"Loaded {len(ds):,} rows  columns: {list(ds.columns)}")

# =========================
# Constants
# =========================
EXPECTED_KEYS = ["ingredients", "strength", "dosage_form", "qualifier", "qualifier_type"]

SALTS = {
    "HCL", "HYDROCHLORIDE",
    "SODIUM", "POTASSIUM",
    "MALEATE", "SUCCINATE",
    "PHOSPHATE", "TARTRATE",
    "BESYLATE", "MESYLATE",
    "ACETATE", "FUMARATE",
    "CITRATE", "CALCIUM", "BROMIDE",
    "SULFATE", "SULPHATE",
    "GLUCONATE", "NITRATE", "VALERATE",
    # counter-ions / anions that pollute basename when split from compound names
    "CHLORIDE", "OXIDE", "HYDROXIDE",
}

DOSAGE_FORMS = {
    "TABLET", "TAB", "TABS", "CAPSULE", "CAP", "CAPS", "CREAM", "GEL",
    "SOLUTION", "SUSPENSION", "SYRUP", "SPRAY", "POWDER",
    "INJ", "SOL", "SOLN", "AMP", "SUSP", "OINT", "OINTMENT",
    "LOTION", "PATCH", "DROP", "DROPS", "LOZENGE", "ELIXIR",
    "EMULSION", "GRANULE", "GRANULES", "VIAL", "AMPOULE",
}

ROUTES = {
    "ORAL", "IV", "INTRAVENOUS", "IM", "INTRAMUSCULAR",
    "SC", "SUBCUTANEOUS", "INJECTION", "INFUSION",
    "TOPICAL", "INHALATION", "OPHTHALMIC", "NASAL", "PO",
}

BASENAME_NOISE = {
    "FOR", "WITH", "AND", "THE", "USP", "NF", "BP", "EP",
    "PRN", "NTE", "VIA", "PER", "USE", "NOT", "NEB", "NEBS",
    # numeric/unit words that leak into basename
    "PERCENT", "MG", "MCG", "ML", "MEQ", "MG",
    # descriptor words that are not part of the drug name
    "NORMAL", "LOT", "FLUSH", "INFUSION", "CONCENTRATE",
}

COMMON_COMPANY = {
    "PFIZER", "NOVARTIS", "ROCHE", "SANOFI", "MERCK",
    "BAYER", "ASTRAZENECA", "GSK", "MYLAN", "TEVA", "SANDOZ",
    "BRISTOL", "SQUIBB", "ABBVIE",
    # generics / distributors that appear in drug names
    "ACCORD", "HEALTHCARE", "PHARMACEUTICALS", "PHARMACEUTICAL",
    "PHARMA", "CORP", "LTD", "INC", "LLC",
    "NEPHRON", "MEDAC", "HIKMA", "ACTAVIS", "APOTEX",
    "WATSON", "LANNETT", "AMNEAL", "AUROBINDO",
}

NON_INGREDIENT = {
    # descriptors — not chemical entities
    "HUMAN", "HUMANA", "NORMAL", "STERILE", "PURIFIED",
    "UNKNOWN", "UNSPECIFIED", "OTHER", "AQUEOUS", "CONCENTRATE",
    # FAERS codes / generic labels
    "NOS", "INGREDIENT", "INGREDIENTS", "ACTIVE", "INACTIVE",
    # solvents / vehicles
    "WATER", "SALINE",
    # broad non-specific categories that appear in FAERS substance fields
    "HERBALS", "HERBAL", "DEVICE", "DEVICES", "EXCIPIENT", "EXCIPIENTS",
    "SUPPLEMENT", "SUPPLEMENTS", "VITAMINS", "MINERALS",
    # prepositions (French/Spanish/German)
    "DE", "VON", "VAN", "DU", "EL", "LA", "LAS", "LOS",
}

# pyarrow output schema — ingredients/salt are list<string>
_SCHEMA = pa.schema([
    pa.field("medicinal_product", pa.string()),
    pa.field("basename",          pa.string()),
    pa.field("ingredients",       pa.list_(pa.string())),
    pa.field("salt",              pa.list_(pa.string())),
    pa.field("strength",          pa.string()),
    pa.field("dosage_form",       pa.string()),
    pa.field("qualifier",         pa.string()),
    pa.field("qualifier_type",    pa.string()),
    # tracks where ingredients came from: "faers" | "llm" | "bracket" | null
    pa.field("ing_source",        pa.string()),
])

# =========================
# Prompt
# =========================
SYSTEM_PROMPT = (
    "You are an AI system for pharmaceutical text decomposition.\n\n"
    "Priority rules:\n"
    "1. If 'Active substance' is provided, use it as the PRIMARY source for ingredients.\n"
    "   Do NOT override it with text from the drug name.\n"
    "2. If 'Active substance' is empty, extract ingredients from the drug name only if "
    "explicitly written as chemicals.\n"
    "3. Extract strength, dosage_form, qualifier from the drug name regardless.\n\n"
    "General rules:\n"
    "- Salts (e.g., HCL, MALEATE) are part of ingredients list.\n"
    "- Country names, brand names, regions are NOT ingredients — put in qualifier.\n"
    "- Preserve original casing of ingredient names.\n"
)

USER_PROMPT = """
Drug name:
{name}

Active substance (from FAERS — use as PRIMARY source for ingredients if provided):
{substance}

Return VALID JSON ONLY with keys:
{keys}

Rules:
- Missing fields → null
- ingredients must be list or null

Examples:
Input name: TAVOR  substance: (empty)
Output: {{"ingredients":null,"strength":null,"dosage_form":null,"qualifier":null,"qualifier_type":null}}

Input name: HUMALOG  substance: INSULIN LISPRO
Output: {{"ingredients":["INSULIN LISPRO"],"strength":null,"dosage_form":null,"qualifier":null,"qualifier_type":null}}

Input name: DENOSINE (JAPAN)  substance: (empty)
Output: {{"ingredients":null,"strength":null,"dosage_form":null,"qualifier":"JAPAN","qualifier_type":"COUNTRY"}}

Input name: PROPRANOLOL HCL 10MG TABLET  substance: (empty)
Output: {{"ingredients":["PROPRANOLOL","HCL"],"strength":"10MG","dosage_form":"TABLET","qualifier":null,"qualifier_type":null}}

Input name: VALCYTE 450MG TABLET  substance: VALGANCICLOVIR HYDROCHLORIDE
Output: {{"ingredients":["VALGANCICLOVIR","HYDROCHLORIDE"],"strength":"450MG","dosage_form":"TABLET","qualifier":null,"qualifier_type":null}}

Input name: AMOXICILINA + ACIDO CLAVULANICO MEPHA  substance: AMOXICILLIN\\CLAVULANIC ACID
Output: {{"ingredients":["AMOXICILLIN","CLAVULANIC ACID"],"strength":null,"dosage_form":null,"qualifier":null,"qualifier_type":null}}
"""

# =========================
# Helpers
# =========================
def _str_or_none(v):
    """Return None for null-ish values; handles string, list, and scalar."""
    if v is None:
        return None
    # LLM sometimes returns a list for scalar fields — take the first element
    if isinstance(v, (list, tuple)):
        v = v[0] if v else None
        if v is None:
            return None
    s = str(v).strip()
    return None if s.lower() in ("none", "null", "") else s


def _pick_name(row) -> str:
    """Prefer medicinal_product_norm over raw medicinal_product."""
    if "medicinal_product_norm" in row.index:
        norm = row.get("medicinal_product_norm")
        if norm is not None and not (isinstance(norm, float) and pd.isna(norm)):
            ns = str(norm).strip()
            if ns:
                return ns
    raw = row.get("medicinal_product")
    return str(raw).strip() if raw is not None else ""


def _pick_substance(row) -> str:
    """Join active_substance_faers to a single string with | separator.

    active_substance_faers is stored as numpy.ndarray in the parquet.
    An empty array (array([], dtype=object)) must return "" so the pipeline
    correctly falls through to LLM extraction rather than taking the FAERS
    path with a useless '[]' string.
    """
    if "active_substance_faers" not in row.index:
        return ""
    val = row.get("active_substance_faers")
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    # Handle both list and numpy array (parquet loads arrays as ndarray)
    if isinstance(val, (list, np.ndarray)):
        parts = [str(x).strip() for x in val if x is not None and str(x).strip()]
        return " | ".join(parts)   # empty array → "" → _parse_substance returns None
    return str(val).strip()


def _parse_substance(substance_str: str):
    """Parse substance string directly to ingredient parts.

    Splits on FAERS separators (\\, ;, |).
    Each part is kept as one compound — multi-word names like
    'INSULIN LISPRO' are intentionally kept intact.
    Returns None when substance is empty.
    """
    if not substance_str:
        return None
    parts = re.split(r"\\|;|\s+\|\s+", substance_str)
    result = [p.strip() for p in parts
              if p.strip() and p.strip().lower() not in ("none", "null")]
    return result if result else None


# Words that are qualifiers/meta-info, not drug ingredients
_BRACKET_QUALIFIERS = {
    # countries
    "JAPAN", "USA", "UK", "FRANCE", "GERMANY", "ITALY", "SPAIN",
    "CANADA", "AUSTRALIA", "CHINA", "INDIA", "KOREA", "EU", "US",
    "EUROPE", "NORDIC", "ASIA", "GLOBAL",
    # pharmacopoeias / quality standards
    "USP", "NF", "BP", "EP", "JP",
    # drug class / generic labels
    "ANESTHETICS", "GENERAL", "ANALGESICS", "ANTIBIOTICS",
}

# Digit+unit pattern: "(500 ML)", "(0.9%)", "(250MG/5ML)"
_BRACKET_UNIT_RE = re.compile(r"^[\d\s./:%,]+[a-zA-Z]{0,4}$")


def _extract_bracket_ingredients(drug_name: str):
    """Last-resort: extract ingredients from () or [] bracket content in drug name.

    Only called when active_substance_faers is empty AND both LLM ingredients
    and LLM qualifier came back null — meaning the bracket was ignored entirely.

    Handles:
      ABBA (AMOXICILLIN + CLAVULANIC ACID) -> ['AMOXICILLIN', 'CLAVULANIC ACID']
      (CRIZOTINIB)                          -> ['CRIZOTINIB']
      [PEGASPARGASE]                        -> ['PEGASPARGASE']
      (L-ASPARAGINASE)                      -> ['L-ASPARAGINASE']  (L- cleaned later)

    Skips:
      (JAPAN), (USP), (500 ML), (ANESTHETICS, GENERAL)
    """
    brackets = re.findall(r"[\(\[]([^\)\]]+)[\)\]]", drug_name)
    for bc in brackets:
        bc = bc.strip()
        # Too short or digit/unit content
        if len(bc) < 3 or _BRACKET_UNIT_RE.match(bc):
            continue
        bc_upper = bc.upper()
        # Single known qualifier word
        if bc_upper in _BRACKET_QUALIFIERS:
            continue
        # All words are qualifier words (e.g. "ANESTHETICS, GENERAL")
        words = re.split(r"[\s,]+", bc_upper)
        if all(w in _BRACKET_QUALIFIERS for w in words if w):
            continue
        # Only extract when there is an explicit drug-combination separator (+ or ;).
        # Single-item brackets are too risky — brand names, lot numbers, region codes
        # can all appear without a separator. LLM has already had a chance to handle them.
        if not re.search(r"[+;]", bc):
            continue
        parts = re.split(r"\s*[+;]\s*", bc)
        parts = [p.strip() for p in parts
                 if len(p.strip()) > 2 and p.strip().upper() not in _BRACKET_QUALIFIERS]
        if parts:
            return parts
    return None


def derive_basename(text: str) -> str:
    text = text.upper()
    bracket_content = re.findall(r"\(([^)]+)\)", text)
    text_no_brackets = re.sub(r"\(.*?\)", " ", text)
    text_no_brackets = re.sub(r"[^A-Z ]", " ", text_no_brackets)

    def _bn_keep(tok: str) -> bool:
        return (
            len(tok) > 2
            and tok not in SALTS
            and tok not in DOSAGE_FORMS
            and tok not in ROUTES
            and tok not in BASENAME_NOISE
            and tok not in COMMON_COMPANY
        )

    tokens = [t for t in text_no_brackets.split() if _bn_keep(t)]
    if tokens:
        return " ".join(tokens)
    for bc in bracket_content:
        bc_clean = re.sub(r"[^A-Z ]", " ", bc.upper())
        fb_tokens = [t for t in bc_clean.split() if _bn_keep(t)]
        if fb_tokens:
            return " ".join(fb_tokens)
    raw_clean = re.sub(r"\s+", " ", re.sub(r"[^A-Z ]", " ", text)).strip()
    last_tokens = [t for t in raw_clean.split() if _bn_keep(t)]
    if last_tokens:
        return " ".join(last_tokens)
    # partial fallback: strip noise/route/form/company even when all tokens fail strict filter
    partial = [t for t in raw_clean.split()
               if t not in BASENAME_NOISE and t not in ROUTES
               and t not in DOSAGE_FORMS and t not in COMMON_COMPANY]
    return " ".join(partial) if partial else raw_clean


def _clean_ingredient_token(raw: str) -> str:
    """Remove FAERS dot-notation, symbols, and numeric position/stereo tokens.
    Also deduplicates repeated words (e.g. 'THIOGUANINE THIOGUANINE ANHYDROUS').

    Dot-notation Greek letters (.GAMMA., .ALPHA., .BETA., .DELTA.) are position/stereo
    descriptors. We remove them entirely so the remaining INN name stays RxNorm-compatible.
    e.g. .GAMMA.-AMINO-.BETA.-HYDROXYBUTYRIC ACID → AMINO HYDROXYBUTYRIC ACID
         .ALPHA.-TOCOPHEROL → TOCOPHEROL  (but substance-level kept as ALPHA TOCOPHEROL)
    Note: this is applied per-token AFTER _parse_substance splits compounds, so
    multi-word INNs like 'ALPHA TOCOPHEROL' from active_substance_faers are preserved.
    """
    t = raw.strip().upper()
    # Convert .WORD. Greek-letter encoding to the word itself (keeps pharmacological specificity).
    # e.g. .ALPHA.-TOCOPHEROL → ALPHA TOCOPHEROL, .DELTA.9-THC → DELTA9 THC
    t = re.sub(r"\.([A-Z]+)\.", r"\1", t)
    t = re.sub(r"[^A-Z0-9 ]", " ", t)
    seen = set()
    tokens = []
    for tk in t.split():
        if (len(tk) > 1
                and not re.fullmatch(r"\d+", tk)
                and not re.fullmatch(r"\d+[A-Z]{1,2}", tk)
                and not re.fullmatch(r"[A-Z]{1,2}\d+", tk)):
            if tk not in seen:
                seen.add(tk)
                tokens.append(tk)
    return " ".join(tokens).strip()


def split_ingredient_and_salt(parsed_ingredients):
    """Split LLM/substance ingredient list into (active_ingredients, salts)."""
    if not isinstance(parsed_ingredients, list):
        return None, None
    ing, salt = [], []
    for x in parsed_ingredients:
        if not isinstance(x, str):
            continue
        t = _clean_ingredient_token(x)
        if not t or t in NON_INGREDIENT:
            continue
        if t in SALTS:
            salt.append(t)
        else:
            ing.append(t)
    return (ing if ing else None, salt if salt else None)


# =========================
# Inference (sequential)
# =========================
def inference_batch(prompts_data: list) -> list:
    """Call LLM for each (name, substance) pair. Returns raw text responses."""
    results = []
    for name, substance in prompts_data:
        try:
            completion = client.chat.completions.create(
                model=MODEL_NAME,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {
                        "role": "user",
                        "content": USER_PROMPT.format(
                            name=name,
                            substance=substance if substance else "(empty)",
                            keys=EXPECTED_KEYS,
                        ),
                    },
                ],
                temperature=0.0,
                max_tokens=512,
            )
            text = completion.choices[0].message.content.strip()
        except Exception as e:
            logger.error(f"LLM error: {e}")
            text = ""
        results.append(text)
        time.sleep(SLEEP_SEC)
    return results


# =========================
# Run pipeline
# =========================
os.makedirs(BATCH_DIR, exist_ok=True)
total_rows = len(ds)
logger.info(f"Start inference — {total_rows:,} rows, batch_size={BATCH_SIZE}")

for i in tqdm(range(0, total_rows, BATCH_SIZE), desc="batches"):
    batch_file = os.path.join(BATCH_DIR, f"batch_{i:07d}_to_{i+BATCH_SIZE:07d}.parquet")
    if os.path.exists(batch_file):
        logger.info(f"Skip existing: {batch_file}")
        continue

    batch = ds.iloc[i : i + BATCH_SIZE]
    records = []

    raw_keys, prompts_data, names_for_basename, substances = [], [], [], []
    for _, row in batch.iterrows():
        raw_keys.append(str(row["medicinal_product"]) if pd.notna(row["medicinal_product"]) else "")
        name = _pick_name(row)
        substance = _pick_substance(row)
        prompts_data.append((name, substance))
        names_for_basename.append(name)
        substances.append(substance)

    outputs = inference_batch(prompts_data)

    for raw_key, name, substance, out in zip(raw_keys, names_for_basename, substances, outputs):
        try:
            parsed = json.loads(out)
        except Exception:
            parsed = {}
        if isinstance(parsed, list):
            parsed = parsed[0] if parsed and isinstance(parsed[0], dict) else {}
        if not isinstance(parsed, dict):
            parsed = {}

        # Priority 1: active_substance_faers (ground truth — bypass LLM)
        # Priority 2: LLM extracted ingredients
        # Priority 3: bracket combination fallback (explicit + or ; only)
        #   — runs regardless of path when ingredients still null after 1 & 2
        substance_parts = _parse_substance(substance)
        qualifier_from_llm = _str_or_none(parsed.get("qualifier"))
        if substance_parts:
            ingredient_clean, salt = split_ingredient_and_salt(substance_parts)
            ing_source = "faers"
        else:
            ingredient_clean, salt = split_ingredient_and_salt(parsed.get("ingredients"))
            ing_source = "llm" if ingredient_clean is not None else None

        # Bracket fallback: applies after both FAERS and LLM paths
        # Only when ingredients still null and LLM didn't identify a qualifier
        if ingredient_clean is None and qualifier_from_llm is None:
            bracket_parts = _extract_bracket_ingredients(name)
            if bracket_parts:
                ingredient_clean, salt = split_ingredient_and_salt(bracket_parts)
                ing_source = "bracket"

        records.append({
            "medicinal_product": raw_key,
            "basename":          derive_basename(name),
            "ingredients":       ingredient_clean,
            "salt":              salt,
            "strength":          _str_or_none(parsed.get("strength")),
            "dosage_form":       _str_or_none(parsed.get("dosage_form")),
            "qualifier":         _str_or_none(parsed.get("qualifier")),
            "qualifier_type":    _str_or_none(parsed.get("qualifier_type")),
            "ing_source":        ing_source,
        })

    df_batch = pd.DataFrame(records)
    table = pa.Table.from_pandas(df_batch, schema=_SCHEMA, preserve_index=False)
    pq.write_table(table, batch_file)
    logger.info(f"Saved batch {i}–{i+BATCH_SIZE} → {batch_file}")

# =========================
# Merge batches
# =========================
logger.info("Merging batch files...")
all_batch_files = sorted(
    glob.glob(os.path.join(BATCH_DIR, "batch_*.parquet")),
    key=lambda x: int(re.search(r"batch_(\d+)_", x).group(1)),
)
logger.info(f"Found {len(all_batch_files)} batch files")

all_tables = [pq.read_table(f) for f in all_batch_files]
final_table = pa.concat_tables(all_tables)
pq.write_table(final_table, OUTPUT_PARQUET)
logger.info(f"Done → {OUTPUT_PARQUET}  ({final_table.num_rows:,} rows)")
