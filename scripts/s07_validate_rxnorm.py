#!/usr/bin/env python3
"""
RxNorm validation pass for s07 LLM output.

For each unique ingredient, queries RxNorm REST API and adds:
  - rxcui          : RxNorm concept ID (null if not found)
  - rxnorm_name    : canonical RxNorm name (null if not found)
  - rxnorm_status  : "exact" | "approx" | "not_found"

Run AFTER s07_openai_run.py. Results are written to a separate parquet
so the original output is never overwritten.

Usage:
  python s07_validate_rxnorm.py
"""

import logging
import time
import urllib.parse

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

# =========================
# Config
# =========================
INPUT_PARQUET  = "/ist-project/scads/thanyathonk/data/pediatric_drugs_llm_cleaned_full_data.parquet"
OUTPUT_PARQUET = "/ist-project/scads/thanyathonk/data/pediatric_drugs_llm_validated.parquet"

RXNORM_BASE    = "https://rxnav.nlm.nih.gov/REST"
SLEEP_SEC      = 0.12   # respect NLM rate limit (~8 req/s)
MAX_RETRIES    = 3

logging.basicConfig(format="%(asctime)s -%(levelname)s- %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# =========================
# RxNorm helpers
# =========================
_session = requests.Session()
_session.headers.update({"Accept": "application/json"})


def _rxnorm_lookup(name: str) -> dict:
    """Query RxNorm for a single ingredient name.
    Returns dict with keys: rxcui, rxnorm_name, rxnorm_status
    Tries exact match first, then approximate (search=2).
    """
    encoded = urllib.parse.quote(name)

    for attempt in range(MAX_RETRIES):
        try:
            # 1. Exact match
            r = _session.get(f"{RXNORM_BASE}/rxcui.json?name={encoded}", timeout=10)
            r.raise_for_status()
            data = r.json()
            rxcui_list = data.get("idGroup", {}).get("rxnormId", [])
            if rxcui_list:
                rxcui = rxcui_list[0]
                # Get canonical name
                r2 = _session.get(f"{RXNORM_BASE}/rxcui/{rxcui}/property.json?propName=RxNorm%20Name", timeout=10)
                canon = None
                if r2.ok:
                    props = r2.json().get("propConceptGroup", {}).get("propConcept", [])
                    canon = props[0]["propValue"] if props else None
                return {"rxcui": rxcui, "rxnorm_name": canon, "rxnorm_status": "exact"}

            # 2. Approximate match
            time.sleep(SLEEP_SEC)
            r = _session.get(f"{RXNORM_BASE}/approximateTerm.json?term={encoded}&maxEntries=1", timeout=10)
            r.raise_for_status()
            cands = r.json().get("approximateGroup", {}).get("candidate", [])
            if cands:
                rxcui = cands[0]["rxcui"]
                rname = cands[0].get("name", None)
                return {"rxcui": rxcui, "rxnorm_name": rname, "rxnorm_status": "approx"}

            return {"rxcui": None, "rxnorm_name": None, "rxnorm_status": "not_found"}

        except Exception as e:
            logger.warning(f"RxNorm error for '{name}' (attempt {attempt+1}): {e}")
            time.sleep(2 ** attempt)

    return {"rxcui": None, "rxnorm_name": None, "rxnorm_status": "error"}


# =========================
# Main
# =========================
logger.info(f"Loading {INPUT_PARQUET}")
df = pd.read_parquet(INPUT_PARQUET)
logger.info(f"Loaded {len(df):,} rows")

# Collect all unique ingredient strings to validate
import numpy as np

all_ingredients: set[str] = set()
for cell in df["ingredients"].dropna():
    if isinstance(cell, (list, np.ndarray)):
        for item in cell:
            if isinstance(item, str) and item.strip():
                all_ingredients.add(item.strip().upper())

logger.info(f"Unique ingredients to validate: {len(all_ingredients):,}")

# Query RxNorm for each unique ingredient
rxnorm_cache: dict[str, dict] = {}
for i, ing in enumerate(sorted(all_ingredients)):
    if i % 100 == 0:
        logger.info(f"  {i}/{len(all_ingredients)} — '{ing}'")
    rxnorm_cache[ing] = _rxnorm_lookup(ing)
    time.sleep(SLEEP_SEC)

logger.info("RxNorm lookup complete")

# Log summary
statuses = {}
for v in rxnorm_cache.values():
    s = v["rxnorm_status"]
    statuses[s] = statuses.get(s, 0) + 1
logger.info(f"Status breakdown: {statuses}")
not_found = [k for k, v in rxnorm_cache.items() if v["rxnorm_status"] == "not_found"]
logger.info(f"Not found ({len(not_found)}): {not_found[:30]}")

# Add per-row columns: rxcui list and validation summary
def _row_rxcui(ing_cell):
    if not isinstance(ing_cell, (list, np.ndarray)):
        return None
    cuis = []
    for item in ing_cell:
        if isinstance(item, str):
            info = rxnorm_cache.get(item.strip().upper(), {})
            cuis.append(info.get("rxcui"))
    return cuis if cuis else None

def _row_rxnorm_status(ing_cell):
    """Returns worst-case status across all ingredients in row."""
    if not isinstance(ing_cell, (list, np.ndarray)):
        return None
    statuses_row = []
    for item in ing_cell:
        if isinstance(item, str):
            info = rxnorm_cache.get(item.strip().upper(), {})
            statuses_row.append(info.get("rxnorm_status", "not_found"))
    if not statuses_row:
        return None
    if "not_found" in statuses_row:
        return "partial" if any(s == "exact" for s in statuses_row) else "not_found"
    if "approx" in statuses_row:
        return "approx"
    return "exact"

df["rxcui"]          = df["ingredients"].apply(_row_rxcui)
df["rxnorm_status"]  = df["ingredients"].apply(_row_rxnorm_status)

# Save
_SCHEMA_OUT = pa.schema([
    pa.field("medicinal_product", pa.string()),
    pa.field("basename",          pa.string()),
    pa.field("ingredients",       pa.list_(pa.string())),
    pa.field("salt",              pa.list_(pa.string())),
    pa.field("strength",          pa.string()),
    pa.field("dosage_form",       pa.string()),
    pa.field("qualifier",         pa.string()),
    pa.field("qualifier_type",    pa.string()),
    pa.field("ing_source",        pa.string()),
    pa.field("rxcui",             pa.list_(pa.string())),
    pa.field("rxnorm_status",     pa.string()),
])

table = pa.Table.from_pandas(df, schema=_SCHEMA_OUT, preserve_index=False)
pq.write_table(table, OUTPUT_PARQUET)
logger.info(f"Saved → {OUTPUT_PARQUET}  ({len(df):,} rows)")
logger.info("Done. Rows with rxnorm_status='not_found' should be reviewed manually.")
