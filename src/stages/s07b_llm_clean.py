from __future__ import annotations

"""Stage S07b – Read S07 drug parquet, run in-process LLM decomposition, merge context, write clean parquet.

Reads ``data/staging/s07_split_drug/{cohort}_drugs_full_data.parquet``, runs Qwen chat +
JSON extraction, left-joins S07 list columns, writes
``data/staging/s07b_llm_clean/{cohort}_drugs_clean_full_data.parquet``.

**Text selection per row (norm → regex → LLM)**

1. ``medicinal_product_norm`` (from S07) if non-empty, else ``medicinal_product`` raw.
2. That full string → LLM ``Text:`` (strength/dosage_form tokens still present for extraction).
3. ``regex_preprocess(norm)["clean"]`` → ``derive_basename`` (noise stripped for basename).
4. ``medicinal_product`` raw is kept as the join key in the output.

**Environment (optional)**

- ``LLM_MODEL_NAME`` — default ``Qwen/Qwen2.5-32B-Instruct``
- ``LLM_CACHE_DIR`` or ``HF_CACHE`` — passed to ``from_pretrained(..., cache_dir=...)``
- ``LLM_BATCH_SIZE`` — default ``64``

Requires ``torch`` and ``transformers`` (install on the GPU node running this stage).
"""

import json
import os
import re
import warnings
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import polars as pl
from loguru import logger
from tqdm import tqdm

from src.utils.dq import dq_summary_markdown
from src.utils.io import PipelineContext, stage_output_path, write_manifest

# =============================================================================
# Setup (mirrors standalone script)
# =============================================================================

EXPECTED_KEYS = [
    "ingredients",
    "strength",
    "dosage_form",
    "qualifier",
    "qualifier_type",
]

SALTS = {
    "HCL",
    "HYDROCHLORIDE",
    "SODIUM",
    "POTASSIUM",
    "MALEATE",
    "SUCCINATE",
    "PHOSPHATE",
    "TARTRATE",
    "BESYLATE",
    "MESYLATE",
    "ACETATE",
    "FUMARATE",
    "CITRATE",
    "CALCIUM",
    "BROMIDE",
}

SYSTEM_PROMPT = (
    "You are an AI system for pharmaceutical text decomposition.\n"
    "STRICT STRING-LEVEL EXTRACTION ONLY.\n"
    "Do NOT infer, normalize, guess, or enrich.\n\n"
    "Rules:\n"
    "- Extract ingredients ONLY if explicitly written as chemicals.\n"
    "- Salts (e.g., HCL, MALEATE) are ingredients but will be post-processed.\n"
    "- Country names, brand names, regions are NOT ingredients.\n"
    "- Preserve original casing.\n"
)

USER_PROMPT = """
Text:
{text}

Return VALID JSON ONLY with keys:
{keys}

Rules:
- Use ONLY text explicitly present in input
- Missing fields → null
- ingredients must be list or null

Examples:
Input: TAVOR
Output: {{"ingredients":null,"strength":null,"dosage_form":null,"qualifier":null,"qualifier_type":null}}

Input: DENOSINE (JAPAN)
Output: {{"ingredients":null,"strength":null,"dosage_form":null,"qualifier":"JAPAN","qualifier_type":"COUNTRY"}}

Input: PROPRANOLOL HCL 10MG TABLET
Output: {{"ingredients":["PROPRANOLOL","HCL"],"strength":"10MG","dosage_form":"TABLET","qualifier":null,"qualifier_type":null}}
"""

ROUTES = {
    "ORAL", "IV", "INTRAVENOUS", "IM", "INTRAMUSCULAR",
    "SC", "SUBCUTANEOUS", "INJECTION", "INFUSION",
    "TOPICAL", "INHALATION", "OPHTHALMIC", "NASAL", "PO",
}

DOSAGE_FORMS = {
    "TABLET", "TAB", "TABS", "CAPSULE", "CAP", "CAPS", "CREAM", "GEL",
    "SOLUTION", "SUSPENSION", "SYRUP", "SPRAY", "POWDER",
    "INJ", "SOL", "SOLN", "AMP", "SUSP", "OINT", "OINTMENT",
    "LOTION", "PATCH", "DROP", "DROPS", "LOZENGE", "ELIXIR",
    "EMULSION", "GRANULE", "GRANULES", "VIAL", "AMPOULE",
}

BASENAME_NOISE = {
    "FOR", "WITH", "AND", "THE", "USP", "NF", "BP", "EP",
    "PRN", "NTE", "VIA", "PER", "USE", "NOT", "NEB", "NEBS",
}

COMMON_COMPANY = {
    "PFIZER", "NOVARTIS", "ROCHE", "SANOFI", "MERCK",
    "BAYER", "ASTRAZENECA", "GSK", "MYLAN", "TEVA", "SANDOZ",
    "BRISTOL", "SQUIBB", "ABBVIE",
}

NON_INGREDIENT = {
    # descriptors
    "HUMAN", "HUMANA", "NORMAL", "STERILE", "PURIFIED",
    "UNKNOWN", "UNSPECIFIED", "OTHER", "AQUEOUS", "CONCENTRATE",
    # FAERS codes / generic labels
    "NOS", "INGREDIENT", "INGREDIENTS", "ACTIVE", "INACTIVE",
    # solvents / vehicles
    "WATER", "SALINE",
    # prepositions (French/Spanish/German)
    "DE", "VON", "VAN", "DU", "EL", "LA", "LAS", "LOS",
}

_LLM_REQUIRED = ["medicinal_product", "basename"]
_LLM_OPTIONAL_STRING = (
    "ingredients",
    "salt",
    "strength",
    "dosage_form",
    "qualifier",
    "qualifier_type",
)

_S07_CONTEXT = (
    "medicinal_product_norm",
    "drug_entries",
    "active_substance_faers",
    "drug_dosage_form",
    "drug_authorization_number",
    "action_drug",
)

_LIST_UTF8 = (
    "active_substance_faers",
    "drug_dosage_form",
    "drug_authorization_number",
    "action_drug",
)

_OUTPUT_COLS = [
    "medicinal_product",
    "basename",
    "ingredients",
    "salt",
    "strength",
    "dosage_form",
    "qualifier",
    "qualifier_type",
]


def regex_preprocess(text: str) -> Dict[str, Any]:
    raw = text

    s = text.upper().strip().rstrip(".")

    brackets = re.findall(r"\((.*?)\)", s)

    s = re.sub(r"\(.*?\)", " ", s)

    s = re.sub(r"[^\w\s\-\/]", " ", s)

    s = re.sub(r"\b\d+(\.\d+)?\s?(MG|ML|MCG|G|IU|%)\b", " ", s)
    s = re.sub(r"\b\d+\/\d+\b", " ", s)

    tokens = s.split()

    cleaned_tokens = []
    for t in tokens:
        if t in SALTS:
            continue
        if t in ROUTES:
            continue
        if t in DOSAGE_FORMS:
            continue
        if t in COMMON_COMPANY:
            continue
        if re.match(r"\d+", t):
            continue
        cleaned_tokens.append(t)

    clean_text = " ".join(cleaned_tokens)

    return {
        "raw": raw,
        "clean": clean_text.strip(),
        "brackets": brackets,
    }


def _clean_ingredient_token(raw: str) -> str:
    """Remove FAERS dot-notation, symbols, and numeric position/stereo tokens."""
    t = raw.strip().upper()
    t = re.sub(r"\.([A-Z]+)\.", r"\1", t)
    t = re.sub(r"[^A-Z0-9 ]", " ", t)
    tokens = [
        tk for tk in t.split()
        if len(tk) > 1
        and not re.fullmatch(r"\d+", tk)
        and not re.fullmatch(r"\d+[A-Z]{1,2}", tk)
        and not re.fullmatch(r"[A-Z]{1,2}\d+", tk)
    ]
    return " ".join(tokens).strip()


def split_ingredient_and_salt(
    parsed_ingredients: Any,
) -> Tuple[Optional[str], Optional[str]]:
    if not isinstance(parsed_ingredients, list):
        return None, None

    ing = []
    salt = []

    for x in parsed_ingredients:
        if not isinstance(x, str):
            continue
        t = _clean_ingredient_token(x)
        if not t:
            continue
        if t in NON_INGREDIENT:
            continue
        if t in SALTS:
            salt.append(t)
        else:
            ing.append(t)

    return (
        ing if ing else None,
        salt if salt else None,
    )


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
    return " ".join(last_tokens) if last_tokens else raw_clean


def _inference_batch(model: Any, tokenizer: Any, texts: List[str]) -> List[str]:
    import torch

    messages = []
    for text in texts:
        messages.append(
            [
                {"role": "system", "content": SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": USER_PROMPT.format(
                        text=text,
                        keys=EXPECTED_KEYS,
                    ),
                },
            ],
        )

    prompts = [
        tokenizer.apply_chat_template(
            m,
            add_generation_prompt=True,
            tokenize=False,
        )
        for m in messages
    ]

    with torch.no_grad():
        tokens = tokenizer(
            prompts,
            return_tensors="pt",
            padding=True,
            truncation=True,
        ).to(model.device)

        outputs = model.generate(
            **tokens,
            max_new_tokens=512,
            do_sample=False,
            temperature=0.4,
            eos_token_id=tokenizer.eos_token_id,
        )

        decoded = []
        for i, out in enumerate(outputs):
            decoded.append(
                tokenizer.decode(
                    out[len(tokens["input_ids"][i]) :],
                    skip_special_tokens=True,
                ).strip()
            )

    return decoded


def _load_model_and_tokenizer() -> Tuple[Any, Any]:
    import torch
    from transformers import AutoModelForCausalLM, AutoTokenizer

    device = "cuda" if torch.cuda.is_available() else "cpu"
    model_name = os.environ.get("LLM_MODEL_NAME", "Qwen/Qwen2.5-32B-Instruct")
    cache_raw = os.environ.get("LLM_CACHE_DIR") or os.environ.get("HF_CACHE") or ""
    cache_dir = cache_raw.strip() or None

    logger.info("[S07b] Loading model %s (device=%s)", model_name, device)
    tokenizer = AutoTokenizer.from_pretrained(model_name, cache_dir=cache_dir)

    if device == "cuda":
        model = AutoModelForCausalLM.from_pretrained(
            model_name,
            torch_dtype=torch.float16,
            device_map=device,
            cache_dir=cache_dir,
        )
    else:
        model = AutoModelForCausalLM.from_pretrained(
            model_name,
            torch_dtype=torch.float32,
            cache_dir=cache_dir,
        )
        model = model.to(device)

    tokenizer.pad_token = tokenizer.eos_token
    tokenizer.padding_side = "left"
    logger.info("[S07b] Model loaded")
    return model, tokenizer


def _resolve_llm_text(row: "pd.Series", has_norm: bool) -> str:
    """Return the text sent to LLM: medicinal_product_norm first, else raw."""
    if has_norm:
        norm = row.get("medicinal_product_norm")
        if norm is not None and not (isinstance(norm, float) and pd.isna(norm)):
            ns = str(norm).strip()
            if ns:
                return ns
    raw = row.get("medicinal_product")
    return str(raw).strip() if raw is not None else ""


def _llm_decompose_dataframe(
    ds: pd.DataFrame,
    model: Any,
    tokenizer: Any,
    batch_size: int,
) -> pl.DataFrame:
    """
    Per-row logic:
      1. norm priority: medicinal_product_norm (S07) if set, else medicinal_product
      2. full norm string → LLM Text: block (strength/dosage_form visible for extraction)
      3. regex_preprocess(norm)["clean"] → derive_basename (noise stripped)
      4. medicinal_product raw stays as join key
    """
    records: List[Dict[str, Any]] = []
    has_norm = "medicinal_product_norm" in ds.columns

    for i in tqdm(range(0, len(ds), batch_size), desc="S07b LLM batches"):
        batch = ds.iloc[i : i + batch_size]

        raw_keys: List[str] = []
        llm_texts: List[str] = []
        for _, row in batch.iterrows():
            raw_keys.append(str(row["medicinal_product"]) if pd.notna(row["medicinal_product"]) else "")
            llm_texts.append(_resolve_llm_text(row, has_norm))

        outputs = _inference_batch(model, tokenizer, llm_texts)

        for raw_key, llm_text, out in zip(raw_keys, llm_texts, outputs):
            try:
                parsed = json.loads(out)
            except Exception:
                parsed = {}

            if isinstance(parsed, list):
                parsed = parsed[0] if parsed and isinstance(parsed[0], dict) else {}

            if not isinstance(parsed, dict):
                parsed = {}

            ingredient_clean, salt = split_ingredient_and_salt(parsed.get("ingredients"))

            def _str_or_none(v: Any) -> Any:
                if v is None:
                    return None
                s = str(v).strip()
                return None if s.lower() in ("none", "null", "") else s

            records.append(
                {
                    "medicinal_product": raw_key,
                    "basename": derive_basename(llm_text),
                    "ingredients": ingredient_clean,
                    "salt": salt,
                    "strength": _str_or_none(parsed.get("strength")),
                    "dosage_form": _str_or_none(parsed.get("dosage_form")),
                    "qualifier": _str_or_none(parsed.get("qualifier")),
                    "qualifier_type": _str_or_none(parsed.get("qualifier_type")),
                },
            )

    pdf = pd.DataFrame(records)[_OUTPUT_COLS]
    return pl.from_pandas(pdf)


def _validate_llm_columns(df: pl.DataFrame, label: str) -> None:
    missing_llm = [c for c in _LLM_REQUIRED if c not in df.columns]
    if missing_llm:
        raise ValueError(
            f"[S07b] LLM output missing columns {missing_llm} ({label}). Found: {df.columns}"
        )


def _merge_s07_context(
    llm_df: pl.DataFrame,
    s07_path: Path,
    cohort: str,
) -> pl.DataFrame:
    if not s07_path.exists():
        logger.warning(
            f"[S07b] {cohort}: S07 full data not found at {s07_path} — "
            "context columns may be missing."
        )
        return llm_df

    s07 = pl.read_parquet(s07_path)
    key = "medicinal_product"
    take = [key] + [c for c in _S07_CONTEXT if c in s07.columns]
    s07_small = s07.select(take).unique(subset=[key], keep="first")
    overlap = [c for c in take if c != key and c in llm_df.columns]
    if overlap:
        s07_small = s07_small.drop(overlap)
    return llm_df.join(s07_small, on=key, how="left")


def run(ctx: PipelineContext) -> None:
    warnings.filterwarnings("ignore")
    os.environ["TOKENIZERS_PARALLELISM"] = "false"

    input_dir = stage_output_path(ctx, "s07_split_drug")
    output_dir = stage_output_path(ctx, "s07b_llm_clean")
    output_dir.mkdir(parents=True, exist_ok=True)

    batch_size = int(os.environ.get("LLM_BATCH_SIZE", "64"))

    try:
        model, tokenizer = _load_model_and_tokenizer()
    except ImportError as e:
        raise ImportError(
            "[S07b] torch and transformers are required for this stage. "
            "Install on the GPU worker, e.g. pip install torch transformers accelerate."
        ) from e

    manifest_payload: Dict[str, Dict[str, object]] = {}

    for cohort in tqdm(("pediatric", "adult"), desc="S07b cohorts"):
        logger.info(f"[S07b] LLM + merge for {cohort}")

        s07_path = input_dir / f"{cohort}_drugs_full_data.parquet"
        if not s07_path.is_file():
            logger.error("[S07b] Missing S07 input: %s — run s07_split_drug first.", s07_path)
            raise FileNotFoundError(f"[S07b] Required file not found: {s07_path}")

        logger.info("[S07b] Loading dataset %s", s07_path)
        ds = pd.read_parquet(s07_path)
        if "medicinal_product" not in ds.columns:
            raise ValueError(f"[S07b] {s07_path} must contain medicinal_product")

        df = _llm_decompose_dataframe(ds, model, tokenizer, batch_size)
        _validate_llm_columns(df, str(s07_path))

        df = _merge_s07_context(df, s07_path, cohort)

        n = df.height

        for col in _LLM_OPTIONAL_STRING:
            if col not in df.columns:
                df = df.with_columns(pl.lit(None, dtype=pl.Utf8).alias(col))

        if "medicinal_product_llm_clean" not in df.columns:
            df = df.with_columns(pl.col("basename").alias("medicinal_product_llm_clean"))

        if "medicinal_product_norm" not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=pl.Utf8).alias("medicinal_product_norm"))

        for col in _LIST_UTF8:
            if col not in df.columns:
                df = df.with_columns(
                    pl.Series(col, [[] for _ in range(n)], dtype=pl.List(pl.Utf8))
                )
            else:
                df = df.with_columns(
                    pl.when(pl.col(col).is_null())
                    .then(pl.Series(col, [[] for _ in range(n)], dtype=pl.List(pl.Utf8)))
                    .otherwise(pl.col(col))
                    .alias(col)
                )

        if "drug_entries" not in df.columns:
            df = df.with_columns(
                pl.Series("drug_entries", [[] for _ in range(n)], dtype=pl.List(pl.Int64))
            )
        else:
            df = df.with_columns(
                pl.when(pl.col("drug_entries").is_null())
                .then(pl.Series("drug_entries", [[] for _ in range(n)], dtype=pl.List(pl.Int64)))
                .otherwise(pl.col("drug_entries"))
                .alias("drug_entries")
            )

        cast_exprs = [
            pl.col("medicinal_product").cast(pl.Utf8, strict=False),
            pl.col("medicinal_product_norm").cast(pl.Utf8, strict=False),
            pl.col("basename").cast(pl.Utf8, strict=False),
            pl.col("medicinal_product_llm_clean").cast(pl.Utf8, strict=False),
        ]
        cast_exprs.extend(
            pl.col(col).cast(pl.Utf8, strict=False) for col in _LLM_OPTIONAL_STRING
        )
        df = df.with_columns(cast_exprs)

        df_clean = df.filter(
            pl.col("basename").is_not_null()
            & (pl.col("basename").str.strip_chars().str.len_chars() > 0)
        )

        df_clean = df_clean.with_columns(
            pl.when(pl.col("ingredients").is_null())
            .then(pl.lit(""))
            .otherwise(pl.col("ingredients").str.strip_chars())
            .alias("ingredients")
        )

        out_path = output_dir / f"{cohort}_drugs_clean_full_data.parquet"
        df_clean.write_parquet(out_path, compression="zstd")

        coverage_pct = df_clean.height / df.height * 100 if df.height > 0 else 0

        dq_summary_markdown(
            ctx,
            "s07b_llm_clean",
            cohort,
            df.height,
            df_clean.height,
            {"llm_basename_coverage_pct": f"{coverage_pct:.2f}"},
        )

        logger.info(
            f"[S07b] {cohort}: {df_clean.height:,}/{df.height:,} rows with non-empty basename "
            f"({coverage_pct:.1f}%)"
        )

        manifest_payload[cohort] = {
            "input_rows": df.height,
            "cleaned_rows": df_clean.height,
            "coverage_pct": coverage_pct,
            "output_path": str(out_path),
        }

    write_manifest(
        ctx,
        "s07b_llm_clean",
        {"stage": "s07b_llm_clean", "cohorts": manifest_payload},
    )
    logger.success("[S07b] LLM decomposition + merge complete")


__all__ = ["run"]
