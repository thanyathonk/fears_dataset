#!/usr/bin/env python
# coding: utf-8

"""Stage S01 – Fetch and parse OpenFDA Drug Event data.

Downloads quarterly FAERS JSON archives from the OpenFDA API, extracts them
in memory, and writes flattened CSV shards for report, patient, drug,
drug-openfda, and reaction entities.  Each shard corresponds to one FDA
partition file.

Output directory: ``data/openFDA_drug_event/{entity}/``
"""

import os
import io
import urllib
import requests
import zipfile
import json
import logging
import time
import numpy as np
import pandas as pd
from pandas import json_normalize
# Load config for network and download tuning (importing settings also loads .env)
try:
    from src.settings import load_settings
    _cfg = load_settings()
    _timeout = 60
    _retries = int(getattr(_cfg.openfda, "retry_attempts", 5))
    _backoff = float(getattr(_cfg.openfda, "retry_backoff_seconds", 2.5))
    _chunk_size = int(getattr(_cfg.openfda, "chunk_size", 4_194_304))
except Exception:
    _timeout = 60
    _retries = 5
    _backoff = 2.5
    _chunk_size = 4_194_304


# API key — populated at import time; validated inside run()
api_token: str = os.environ.get("OPENFDA_API_KEY", "")

# Mutable dicts shared between module-scope functions and run().
# run() populates them before launching workers.
headers: dict = {}
attribute_map: dict = {}
_out_paths: dict = {}  # keys: out, out_meta, out_report, out_patient, ...

# Fastest gzip level for intermediate files (speed >> compression ratio)
_GZIP_FAST = {"method": "gzip", "compresslevel": 1}

_log = logging.getLogger(__name__)

# Age unit conversion constants (FAERS reports age in various units)
AGE_MONTHS_PER_YEAR = 12.0
AGE_WEEKS_PER_YEAR = 52.0
AGE_DAYS_PER_YEAR = 365.0
AGE_HOURS_PER_YEAR = 8760.0


def _apply_enum_map(series: pd.Series, value_dict: dict) -> pd.Series:
    """Vectorized replacement for: series.apply(lambda x: str(int(x)) if x >= 0 else x).map(value_dict)"""
    num = pd.to_numeric(series, errors="coerce")
    valid = num.notna() & (num >= 0)
    keys = num.astype(object)
    keys[valid] = num[valid].astype(int).astype(str)
    return keys.map(value_dict)


def _apply_enum_map_padded(series: pd.Series, value_dict: dict, width: int = 3) -> pd.Series:
    """Vectorized zero-padded enum map (for drugadministrationroute)."""
    num = pd.to_numeric(series, errors="coerce")
    valid = num.notna() & (num >= 0)
    keys = num.astype(object)
    keys[valid] = num[valid].astype(int).astype(str).str.zfill(width)
    return keys.map(value_dict)


def _http_get_json(url: str, timeout: int, max_retries: int, backoff_seconds: float) -> dict:
    attempt = 0
    last_exc = None
    while attempt < max_retries:
        try:
            resp = requests.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            last_exc = exc
            attempt += 1
            time.sleep(min(backoff_seconds * (2 ** attempt), 60))
    raise last_exc


import yaml

def _http_get_text(url: str, timeout: int, max_retries: int, backoff_seconds: float) -> str:
    attempt = 0
    last_exc = None
    while attempt < max_retries:
        try:
            resp = requests.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp.text
        except Exception as exc:
            last_exc = exc
            attempt += 1
            time.sleep(min(backoff_seconds * (2 ** attempt), 60))
    raise last_exc



def request_and_generate_data(drug_event_file, headers=None, stream=True, timeout=_timeout, max_retries=_retries, backoff_seconds=_backoff, chunk_size=_chunk_size):
    """Download a single FAERS partition ZIP, extract JSON in-memory, and return parsed data.

    Returns:
        tuple: (data_dict, download_time_seconds, download_size_bytes)

    Raises:
        The last exception if all retry attempts fail.
    """
    # headers=None sentinel: use module-level dict (populated by run() before workers start)
    if headers is None:
        headers = globals()["headers"]
    t_download_start = time.time()
    attempt = 0
    last_exc = None
    while attempt < max_retries:
        try:
            with requests.get(drug_event_file, headers=headers, stream=True, timeout=timeout) as response:
                response.raise_for_status()
                download_size_bytes = int(response.headers.get("Content-Length", 0))
                # Stream into in-memory buffer — avoids writing/reading a temp file on disk
                buf = io.BytesIO()
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        buf.write(chunk)
            t_download_end = time.time()
            download_time = t_download_end - t_download_start

            buf.seek(0)
            with zipfile.ZipFile(buf) as zf:
                first_file = zf.namelist()[0]
                with zf.open(first_file) as f:
                    data = json.load(io.TextIOWrapper(f, encoding="utf-8"))
            return data, download_time, download_size_bytes
        except Exception as exc:
            last_exc = exc
            attempt += 1
            time.sleep(min(backoff_seconds * (2 ** attempt), 60))
    raise last_exc


def report_formatter(df):
    """Apply OpenFDA enum mappings to top-level report columns (e.g. serious, receiptdateformat)."""
    attributes_dict = attribute_map["properties"]
    cols = np.intersect1d(list(attributes_dict.keys()), df.columns)
    for col in cols:
        try:
            if attributes_dict[col]["possible_values"]["type"] == "one_of":
                df[col] = _apply_enum_map(df[col], attributes_dict[col]["possible_values"]["value"])
        except Exception as e:
            _log.debug("report_formatter: skipping enum map for column '%s': %s", col, e)
    return df


def primarysource_formatter(df):
    """Apply OpenFDA enum mappings to primarysource.* columns (e.g. qualification)."""
    keyword = "primarysource"
    attributes_dict = attribute_map["properties"][keyword]["properties"]
    cols = np.intersect1d(
        list(attributes_dict.keys()), [x.replace(keyword + ".", "") for x in df.columns]
    )
    for col in cols:
        try:
            if attributes_dict[col]["possible_values"]["type"] == "one_of":
                full_col = keyword + "." + col
                df[full_col] = _apply_enum_map(df[full_col], attributes_dict[col]["possible_values"]["value"])
        except Exception as e:
            _log.debug("primarysource_formatter: skipping enum map for column '%s': %s", col, e)
    return df


def report_serious_formatter(df):
    """Apply OpenFDA enum mapping to the 'serous' (seriousness) column."""
    col = "serous"
    try:
        value_dict = attribute_map["properties"][col]["possible_values"]["value"]
        df[col] = _apply_enum_map(df[col], value_dict)
    except Exception as e:
        _log.debug("report_serious_formatter: skipping 'serous' enum map: %s", e)
    return df


def patient_formatter(df):
    """Apply OpenFDA enum mappings to patient.* columns and compute master_age in years.

    Converts age from various units (Month, Week, Day, Hour, Decade) to whole years
    using floor division, then joins the computed ``master_age`` column back to the DataFrame.
    """
    attributes_dict = attribute_map["properties"]["patient"]["properties"]
    cols = np.intersect1d(
        list(attributes_dict.keys()), [x.replace("patient.", "") for x in df.columns]
    )
    for col in cols:
        try:
            if attributes_dict[col]["possible_values"]["type"] == "one_of":
                full_col = "patient." + col
                df[full_col] = _apply_enum_map(df[full_col], attributes_dict[col]["possible_values"]["value"])
        except Exception as e:
            _log.debug("patient_formatter: skipping enum map for column '%s': %s", col, e)
        if "date" in col:
            df[col] = pd.to_datetime(df[col], infer_datetime_format=True)

    aged = df.copy()
    aged = aged[["patient.patientonsetage", "patient.patientonsetageunit"]].dropna()
    year_reports = (
        aged[aged["patient.patientonsetageunit"].astype(str) == "Year"]
    ).index.values
    month_reports = (
        aged[aged["patient.patientonsetageunit"].astype(str) == "Month"]
    ).index.values
    day_reports = (
        aged[aged["patient.patientonsetageunit"].astype(str) == "Day"]
    ).index.values
    decade_reports = (
        aged[aged["patient.patientonsetageunit"].astype(str) == "Decade"]
    ).index.values
    week_reports = (
        aged[aged["patient.patientonsetageunit"].astype(str) == "Week"]
    ).index.values
    hour_reports = (
        aged[aged["patient.patientonsetageunit"].astype(str) == "Hour"]
    ).index.values

    aged["master_age"] = np.nan

    aged.loc[year_reports, "master_age"] = aged.loc[
        year_reports, "patient.patientonsetage"
    ].astype(int)
    aged.loc[month_reports, "master_age"] = np.floor(
        aged.loc[month_reports, "patient.patientonsetage"].astype(int) / AGE_MONTHS_PER_YEAR
    )
    aged.loc[week_reports, "master_age"] = np.floor(
        aged.loc[week_reports, "patient.patientonsetage"].astype(int) / AGE_WEEKS_PER_YEAR
    )
    aged.loc[day_reports, "master_age"] = np.floor(
        aged.loc[day_reports, "patient.patientonsetage"].astype(int) / AGE_DAYS_PER_YEAR
    )
    aged.loc[decade_reports, "master_age"] = (
        aged.loc[decade_reports, "patient.patientonsetage"].astype(int) * 10
    )
    aged.loc[hour_reports, "master_age"] = np.floor(
        aged.loc[hour_reports, "patient.patientonsetage"].astype(int) / AGE_HOURS_PER_YEAR
    )

    return df.join(aged[["master_age"]])


# ### parse patient.drug data formatting/mapping function

# #### helpers for nested field flattening

def _flatten_activesubstance(val) -> "str | None":
    """Flatten the nested `activesubstance` field to a '|'-separated string of names.

    FAERS raw JSON stores activesubstance as:
      - a dict:  {"activesubstancename": "ASPIRIN"}
      - a list:  [{"activesubstancename": "A"}, {"activesubstancename": "B"}]
      - sometimes already a plain string (rare)

    Returns a single string like "ASPIRIN" or "A|B", or None if empty.
    """
    if val is None:
        return None
    if isinstance(val, dict):
        name = val.get("activesubstancename")
        return str(name).strip() if name else None
    if isinstance(val, list):
        names = [
            a.get("activesubstancename", "").strip()
            for a in val
            if isinstance(a, dict) and a.get("activesubstancename")
        ]
        return "|".join(names) if names else None
    if isinstance(val, str) and val.strip():
        return val.strip()
    return None


def patient_drug_formatter(df):
    attributes_dict = attribute_map["properties"]["patient"]["properties"]["drug"][
        "items"
    ]["properties"]
    cols = np.intersect1d(list(attributes_dict.keys()), df.columns)
    for col in cols:
        try:
            if attributes_dict[col]["possible_values"]["type"] == "one_of":
                value_dict = attributes_dict[col]["possible_values"]["value"]
                if col == "drugadministrationroute":
                    df[col] = _apply_enum_map_padded(df[col], value_dict, width=3)
                else:
                    df[col] = _apply_enum_map(df[col], value_dict)
        except Exception as e:
            _log.debug("patient_drug_formatter: skipping enum map for column '%s': %s", col, e)
    return df


def parse_patient_drug_data(results):
    """Parse patient.drug records into a flat DataFrame.

    Unit: 1 row = 1 (safetyreportid, entry) drug record.
    `entry` is the 0-based index of the drug within the report.

    Key output columns:
      - safetyreportid, entry                  — primary key
      - medicinalproduct                        — raw drug name
      - drugcharacterization                    — suspect/concomitant/interacting
      - drugadministrationroute                 — route (enum-decoded)
      - drugindication                          — indication text
      - drugdosagetext, drugdosageform          — dosage details
      - drugauthorizationnumb, drugbatchnumb    — regulatory IDs
      - drugstructuredosagenumb/unit            — structured dose
      - drugtreatmentduration/unit              — treatment duration
      - activesubstance_name                    — NEW: flattened from nested activesubstance
      - actiondrug                              — action taken (e.g., withdrawn)

    Note: 'openfda' is excluded here — handled by parse_patient_drug_openfda_data().
    """
    records = []
    for rid, drug_cell in zip(results["safetyreportid"].values, results["patient.drug"].values):
        if isinstance(drug_cell, dict):
            items = [drug_cell]
        elif isinstance(drug_cell, list):
            items = drug_cell
        elif isinstance(drug_cell, np.ndarray):
            items = drug_cell.tolist()
        else:
            continue

        for i, drug in enumerate(items):
            entry_dict: dict = {}
            for k, v in drug.items():
                if k == "openfda":
                    # Handled separately by parse_patient_drug_openfda_data()
                    continue
                elif k == "activesubstance":
                    # Preserve raw for S02 parsing + audit; flatten for mapping
                    entry_dict["activesubstance"] = (
                        json.dumps(v) if isinstance(v, (dict, list)) else (str(v) if v is not None else None)
                    )
                    entry_dict["activesubstance_name"] = _flatten_activesubstance(v)
                elif k == "drugrecurrence" and isinstance(v, (list, dict)):
                    # drugrecurrence is nested (list of dicts); serialize to JSON string
                    try:
                        entry_dict["drugrecurrence_json"] = json.dumps(v)
                    except Exception:
                        entry_dict["drugrecurrence_json"] = None
                else:
                    entry_dict[k] = v

            entry_dict["safetyreportid"] = str(rid)
            entry_dict["entry"] = i
            records.append(entry_dict)

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    # Keep only string-named columns (guard against accidental int column names)
    df = df[[c for c in df.columns if isinstance(c, str)]]
    return patient_drug_formatter(df)


def parse_patient_drug_openfda_data(results):
    # Same output schema: columns [key, value, safetyreportid, entry]
    # Avoids per-entry pd.concat by collecting plain dicts first.
    records = []
    for rid, drug_cell in zip(results["safetyreportid"].values, results["patient.drug"].values):
        if isinstance(drug_cell, dict):
            items = [drug_cell]
        elif isinstance(drug_cell, list):
            items = drug_cell
        elif isinstance(drug_cell, np.ndarray):
            items = drug_cell.tolist()
        else:
            continue
        for i, drug in enumerate(items):
            try:
                for key, vals in drug["openfda"].items():
                    val_list = vals if isinstance(vals, list) else [vals]
                    for val in val_list:
                        records.append({"key": key, "value": val, "safetyreportid": str(rid), "entry": i})
            except (KeyError, TypeError, AttributeError):
                pass
    if not records:
        return pd.DataFrame(columns=["key", "value", "safetyreportid", "entry"])
    return pd.DataFrame(records)


def patient_reactions_formatter(df):
    """Apply OpenFDA enum mappings to patient.reaction columns (e.g. reactionoutcome)."""
    attributes_dict = attribute_map["properties"]["patient"]["properties"]["reaction"][
        "items"
    ]["properties"]
    cols = np.intersect1d(list(attributes_dict.keys()), df.columns)
    for col in cols:
        try:
            if attributes_dict[col]["possible_values"]["type"] == "one_of":
                df[col] = _apply_enum_map(df[col], attributes_dict[col]["possible_values"]["value"])
        except Exception as e:
            _log.debug("patient_reactions_formatter: skipping enum map for column '%s': %s", col, e)
    return df


def parse_patient_reaction_data(results):
    records = []
    for rid, rx_cell in zip(results["safetyreportid"].values, results["patient.reaction"].values):
        if isinstance(rx_cell, dict):
            items = [rx_cell]
        elif isinstance(rx_cell, list):
            items = rx_cell
        elif isinstance(rx_cell, np.ndarray):
            items = rx_cell.tolist()
        else:
            continue
        for i, rx in enumerate(items):
            entry = dict(rx)
            entry["safetyreportid"] = str(rid)
            entry["entry"] = i
            records.append(entry)
    if not records:
        return pd.DataFrame()
    return patient_reactions_formatter(pd.DataFrame(records)).reset_index(drop=True)


def parsing_main(drug_event_file, file_index, total_files):
    """Download, parse, and save a single FAERS partition file.

    Handles: metadata, report, patient (with age), patient.drug,
    patient.drug.openfda, and patient.reaction entities.
    Skips files that were already processed (resume-safe).
    """
    # Resolve output paths from the shared dict (populated by run() before workers start)
    out_meta = _out_paths["out_meta"]
    out_report = _out_paths["out_report"]
    out_patient = _out_paths["out_patient"]
    out_patient_drug = _out_paths["out_patient_drug"]
    out_patient_drug_openfda = _out_paths["out_patient_drug_openfda"]
    out_patient_drug_openfda_rxcui = _out_paths["out_patient_drug_openfda_rxcui"]
    out_patient_reaction = _out_paths["out_patient_reaction"]

    t0 = time.time()

    file_lst = drug_event_file.split("/")[2:]
    out_file = "_".join(file_lst[3:]).split(".")[0]

    progress_percent = (file_index + 1) / total_files * 100

    # ── Resume check: skip entirely if all 6 output files already exist ───────
    # Use report file as sentinel (first file written per partition).
    # If it exists, the partition was fully processed in a previous run.
    sentinel = _out_paths["out_report"] + out_file + "_report.csv.gzip"
    all_outputs = [
        _out_paths["out_report"]                  + out_file + "_report.csv.gzip",
        _out_paths["out_patient"]                 + out_file + "_patient.csv.gzip",
        _out_paths["out_patient_drug"]            + out_file + "_patient_drug.csv.gzip",
        _out_paths["out_patient_drug_openfda"]    + out_file + "_patient_drug_openfda.csv.gzip",
        _out_paths["out_patient_drug_openfda_rxcui"] + out_file + "_patient_drug_openfda_rxcui.csv.gzip",
        _out_paths["out_patient_reaction"]        + out_file + "_patient_reaction.csv.gzip",
    ]
    if all(os.path.exists(p) for p in all_outputs):
        print(
            f"[{file_index + 1}/{total_files}] ⏭  {out_file} already done — skipped ({progress_percent:.1f}%)",
            flush=True,
        )
        return

    print(
        f"\n[{file_index + 1}/{total_files}] Downloading {out_file} ({progress_percent:.1f}%)...",
        flush=True,
    )

    try:
        data, download_time, download_size_bytes = request_and_generate_data(
            drug_event_file, stream=True
        )
        download_size_mb = download_size_bytes / (1024 * 1024)
        print(
            f"[{file_index + 1}/{total_files}] {out_file}: "
            f"{download_size_mb:.1f} MB downloaded in {download_time:.1f}s "
            f"({download_size_mb / max(download_time, 1e-6):.1f} MB/s)",
            flush=True,
        )

        # parse metadata
        meta = json_normalize(data["meta"])
        try:
            total_records_in_file = meta.get("results.total", pd.Series(np.nan)).iloc[0]
            if pd.notna(total_records_in_file):
                print(f"  Records in file: {int(total_records_in_file):,}", flush=True)
            else:
                print(f"  Could not determine record count for {out_file}.", flush=True)
        except Exception as e:
            print(f"  Error reading record count for {out_file}: {e}", flush=True)

        meta.to_csv(out_meta + out_file + "_meta.csv.gzip", compression=_GZIP_FAST)
        del meta

        results = json_normalize(data["results"])
        del data  # free raw JSON memory before CPU-intensive parsing

        # parse and output report data
        results.index = results["safetyreportid"].values
        results = results.rename_axis("safetyreportid")
        report = results.drop(["patient.drug", "patient.reaction"], axis=1)

        try:
            report_df = (
                primarysource_formatter(
                    report_formatter(report_serious_formatter(report))
                )
                .drop(
                    report.columns.values[report.columns.str.contains("patient.")],
                    axis=1,
                )
                .reset_index(drop=True)
            )
            report_path = out_report + out_file + "_report.csv.gzip"
            if not os.path.exists(report_path):
                report_df.to_csv(report_path, compression=_GZIP_FAST)
            del report
            del report_df
        except Exception:
            print(f"  could not parse report data in {out_file}", flush=True)

        try:
            patient_df = (
                results.loc[
                    :, results.columns.values[results.columns.str.contains("patient.")]
                ]
            ).drop(["patient.drug", "patient.reaction"], axis=1)
            patient_out = out_patient + out_file + "_patient.csv.gzip"
            if not os.path.exists(patient_out):
                patient_formatter(patient_df).reset_index().to_csv(
                    patient_out, compression=_GZIP_FAST
                )
            del patient_df
        except Exception:
            print(f"  could not parse patient data in {out_file}", flush=True)

        try:
            patientdrug_df = parse_patient_drug_data(results)
            # Validate (safetyreportid, entry) uniqueness per file
            if not patientdrug_df.empty and {"safetyreportid", "entry"}.issubset(patientdrug_df.columns):
                dup = patientdrug_df.duplicated(subset=["safetyreportid", "entry"]).sum()
                if dup > 0:
                    print(f"  ⚠  patient.drug: {dup} duplicate (safetyreportid, entry) in {out_file}", flush=True)
            pd_out = out_patient_drug + out_file + "_patient_drug.csv.gzip"
            if not os.path.exists(pd_out):
                patientdrug_df.reset_index(drop=True).to_csv(pd_out, compression=_GZIP_FAST)
            _drug_rows = len(patientdrug_df)
            del patientdrug_df
            print(f"  patient.drug: {_drug_rows:,} rows", flush=True)
        except Exception as _e:
            print(f"  could not parse patient.drug data in {out_file}: {_e}", flush=True)
            _drug_rows = 0

        try:
            openfdas_df = parse_patient_drug_openfda_data(results).reset_index(drop=True)
            openfda_out = out_patient_drug_openfda + out_file + "_patient_drug_openfda.csv.gzip"
            if not os.path.exists(openfda_out):
                openfdas_df.to_csv(openfda_out, compression=_GZIP_FAST)
            rxcui_out = out_patient_drug_openfda_rxcui + out_file + "_patient_drug_openfda_rxcui.csv.gzip"
            if not os.path.exists(rxcui_out):
                openfdas_df.query('key=="rxcui"').to_csv(rxcui_out, compression=_GZIP_FAST)
            # Summary: openfda row count + distinct keys found
            _openfda_rows = len(openfdas_df)
            _openfda_keys = sorted(openfdas_df["key"].unique().tolist()) if _openfda_rows else []
            del openfdas_df
            print(
                f"  patient.drug.openfda: {_openfda_rows:,} rows  |  keys: {_openfda_keys}",
                flush=True,
            )
        except Exception as _e:
            print(f"  could not parse patient.drug.openfda data in {out_file}: {_e}", flush=True)

        try:
            patientreactions = parse_patient_reaction_data(results)
            rx_out = out_patient_reaction + out_file + "_patient_reaction.csv.gzip"
            if not os.path.exists(rx_out):
                patientreactions.reset_index(drop=True).to_csv(rx_out, compression=_GZIP_FAST)
            del patientreactions
        except Exception:
            print(f"  could not parse patient.reaction data in {out_file}", flush=True)

        t1 = time.time()
        print(
            f"[{file_index + 1}/{total_files}] ✅ {out_file} done in {t1 - t0:.0f}s  (overall: {progress_percent:.1f}%)",
            flush=True,
        )

    except requests.exceptions.ConnectionError:
        print(f"[{file_index + 1}/{total_files}] ❌ Connection error for {out_file}", flush=True)
    except Exception as e:
        print(f"[{file_index + 1}/{total_files}] ❌ Error processing {out_file}: {e}", flush=True)


from joblib import Parallel, delayed
from src.utils.io import PipelineContext, write_manifest
from src.settings import load_settings


def run(ctx: PipelineContext) -> None:
    """CLI entry point — validates config, fetches manifest+YAML, creates dirs, runs downloads."""

    # ── API key ──────────────────────────────────────────────────────────────
    if not api_token:
        raise RuntimeError("OPENFDA_API_KEY is required. Set it in .env or as an env var.")

    # Populate module-level headers dict in place (used by request_and_generate_data)
    headers.clear()
    headers.update({
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_token}",
    })

    # ── Fetch OpenFDA manifest ────────────────────────────────────────────────
    manifest_override = os.environ.get("OPENFDA_MANIFEST_PATH", "").strip()
    if manifest_override and os.path.exists(manifest_override):
        print(f"[S01] Loading manifest from local file: {manifest_override}", flush=True)
        with open(manifest_override, "r", encoding="utf-8") as fh:
            manifest = json.load(fh)
    else:
        print("[S01] Fetching OpenFDA manifest (download.json)...", flush=True)
        try:
            manifest = _http_get_json(
                "https://api.fda.gov/download.json",
                timeout=_timeout,
                max_retries=_retries,
                backoff_seconds=_backoff,
            )
            print("[S01] Manifest loaded successfully.", flush=True)
        except Exception as e:
            raise RuntimeError(
                f"Failed to fetch OpenFDA manifest. Network/DNS issue? Details: {e}"
            ) from e

    total_records = manifest["results"]["drug"]["event"]["total_records"]
    drug_event_files = [x["file"] for x in manifest["results"]["drug"]["event"]["partitions"]]
    print(f"[S01] Total records in OpenFDA: {total_records:,}", flush=True)
    print(f"[S01] Total partition files   : {len(drug_event_files)}", flush=True)

    # ── Fetch field definitions (YAML) ───────────────────────────────────────
    fields_override = os.environ.get("OPENFDA_FIELDS_YAML_PATH", "").strip()
    if fields_override and os.path.exists(fields_override):
        print(f"[S01] Loading field definitions from local file: {fields_override}", flush=True)
        with open(fields_override, "r", encoding="utf-8") as fh:
            loaded_map = yaml.safe_load(fh)
    else:
        print("[S01] Fetching OpenFDA field definitions (drugevent.yaml)...", flush=True)
        try:
            yaml_text = _http_get_text(
                "https://open.fda.gov/fields/drugevent.yaml",
                timeout=_timeout,
                max_retries=_retries,
                backoff_seconds=_backoff,
            )
            loaded_map = yaml.safe_load(yaml_text)
            print("[S01] Field definitions loaded.", flush=True)
        except Exception as e:
            raise RuntimeError(
                f"Failed to fetch OpenFDA field YAML. Network/DNS issue? Details: {e}"
            ) from e

    # Populate module-level attribute_map dict in place (used by all formatter functions)
    attribute_map.clear()
    attribute_map.update(loaded_map)

    # ── Create output directories ─────────────────────────────────────────────
    data_dir = str(ctx.config.paths.data_root) + "/"
    out = data_dir + "openFDA_drug_event/"
    subdirs = ["report", "meta", "patient", "patient_drug",
               "patient_drug_openfda", "patient_drug_openfda_rxcui", "patient_reaction"]
    for d in [out] + [out + s + "/" for s in subdirs]:
        os.makedirs(d, exist_ok=True)

    # Populate module-level _out_paths dict (used by parsing_main worker threads)
    _out_paths.update({
        "out":                        out,
        "out_report":                 out + "report/",
        "out_meta":                   out + "meta/",
        "out_patient":                out + "patient/",
        "out_patient_drug":           out + "patient_drug/",
        "out_patient_drug_openfda":   out + "patient_drug_openfda/",
        "out_patient_drug_openfda_rxcui": out + "patient_drug_openfda_rxcui/",
        "out_patient_reaction":       out + "patient_reaction/",
    })

    # ── Parallel download ─────────────────────────────────────────────────────
    # Rate limit note:
    #   - api.fda.gov: 240 req/min with API key
    #   - download.open.fda.gov: AWS S3/CDN — NOT rate-limited by FDA
    #   - Safe range: 8–20 workers. Override: S01_JOBS=15
    try:
        _default_jobs = int(ctx.config.openfda.parallel_downloads)
    except Exception:
        _default_jobs = 10
    n_jobs = int(os.environ.get("S01_JOBS", str(_default_jobs)))

    total_files = len(drug_event_files)
    est_req_per_min = n_jobs * 2
    print(f"\n[S01] Starting parallel download: {total_files} files", flush=True)
    print(f"[S01] Workers          : {n_jobs}  (S01_JOBS env to override)", flush=True)
    print(f"[S01] Est. req/min     : ~{est_req_per_min}  (API key limit: 240/min)", flush=True)
    print(f"[S01] Output           : {out}", flush=True)
    print("=" * 60, flush=True)

    t0 = time.time()
    Parallel(n_jobs=n_jobs, prefer="threads")(
        delayed(parsing_main)(drug_event_file, i, total_files)
        for i, drug_event_file in enumerate(drug_event_files)
    )
    elapsed = time.time() - t0

    print(f"\n[S01] ✅ All {total_files} files done in {elapsed:.0f}s ({elapsed/60:.1f} min)", flush=True)

    write_manifest(ctx, "s01_fetch_openfda", {
        "stage": "s01_fetch_openfda",
        "total_records": total_records,
        "total_files": total_files,
        "output_dir": out,
        "elapsed_seconds": round(elapsed, 1),
    })


__all__ = ["run"]
