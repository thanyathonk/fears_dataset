from __future__ import annotations

"""Alternative Stage S02 implementation using Polars streaming transforms.

This module reads the OpenFDA flat CSV outputs emitted by S01 and produces
normalized ER tables as Parquet files under the S02 staging directory.
It replicates the key tables consumed by later pipeline stages while avoiding
large in-memory pandas concatenations.
"""

import gc
import time
from pathlib import Path
from typing import Dict, Iterable, Mapping, Sequence

if __package__ is None or __package__ == "":
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import os
import subprocess
import sys
from multiprocessing import cpu_count

import polars as pl
from joblib import Parallel, delayed
from loguru import logger
from tqdm import tqdm

try:
    import pandas as pd
    _HAS_PANDAS = True
except ImportError:
    _HAS_PANDAS = False

try:
    import psutil as _psutil
    _PSUTIL_PROC = _psutil.Process(os.getpid())
    _HAS_PSUTIL = True
except Exception:
    _HAS_PSUTIL = False
    _PSUTIL_PROC = None  # type: ignore

from src.utils.io import PipelineContext, stage_output_path, write_manifest

__all__ = ["run"]

_NULLS = ["", "NA", "NaN", "null", "NULL"]
_PATTERNS = ("*.csv", "*.csv.gz", "*.csv.gzip")


def _use_high_memory_openfda(ctx: PipelineContext) -> bool:
    """True = in-memory openfda path (~80–120GB RAM). Env S02_OPENFDA_HIGH_MEMORY=1 or config."""
    env_val = os.environ.get("S02_OPENFDA_HIGH_MEMORY", "").strip().lower()
    if env_val in ("1", "true", "yes"):
        return True
    stages = getattr(ctx.config, "stages", None)
    return bool(getattr(stages, "s02_openfda_high_memory", False) if stages else False)


def _openfda_root(ctx: PipelineContext) -> Path:
    configured = ctx.config.metadata.get("openfda_root")
    if configured:
        root = Path(configured)
        if not root.is_absolute():
            root = (ctx.config.paths.root / root).resolve()
    else:
        root = (ctx.config.paths.data_root / "openFDA_drug_event").resolve()
    if not root.exists():
        raise FileNotFoundError(f"OpenFDA directory not found: {root}")
    return root


def _is_gzip_ok(path: Path) -> bool:
    """Return False if file is truncated/corrupt gzip. Used only for diagnosis, not hot path."""
    if not str(path).endswith((".gz", ".gzip")):
        return True
    try:
        r = subprocess.run(
            ["gzip", "-t", str(path)],
            capture_output=True,
            timeout=10,
        )
        return r.returncode == 0
    except Exception:
        return False


def _read_and_aggregate_openfda_file(
    file_path: Path,
    known_keys: list,
) -> "pl.DataFrame | None":
    """Read gzip CSV with Polars, aggregate by (safetyreportid, entry, key). Returns Polars DataFrame or None.

    Uses Polars for ~2-5× speedup vs pandas; no subprocess gzip -t needed since
    Polars handles corrupt gzip gracefully via exception catching.
    """
    try:
        needed = ["safetyreportid", "entry", "key", "value"]
        df = pl.read_csv(
            file_path,
            columns=needed,
            schema_overrides={c: pl.Utf8 for c in needed},
            null_values=_NULLS,
            ignore_errors=True,
            infer_schema_length=0,
        )
        if df.is_empty() or not set(needed).issubset(df.columns):
            return None
        df = df.filter(
            pl.col("safetyreportid").is_not_null()
            & pl.col("key").is_not_null()
            & pl.col("value").is_not_null()
            & pl.col("key").is_in(known_keys)
        )
        if df.is_empty():
            return None
        df = df.with_columns(
            pl.col("entry").cast(pl.Int64, strict=False).fill_null(0)
        )
        return (
            df.group_by(["safetyreportid", "entry", "key"])
            .agg(pl.col("value").drop_nulls().unique().alias("values"))
        )
    except Exception:
        return None


def _diagnose_openfda_file(file_path: Path, known_keys: list) -> str | None:
    """Return None if file is OK, else a short reason string. Used for diagnostics."""
    if not _HAS_PANDAS:
        return "no_pandas"
    if not _is_gzip_ok(file_path):
        return "gzip_corrupt_or_truncated"
    try:
        pdf = pd.read_csv(
            file_path,
            compression="gzip" if str(file_path).endswith((".gz", ".gzip")) else "infer",
            on_bad_lines="skip",
            dtype=str,
            encoding="utf-8",
            encoding_errors="replace",
            low_memory=False,
        )
        needed = {"safetyreportid", "entry", "key", "value"}
        if pdf.empty:
            return "empty_csv"
        if not needed.issubset(pdf.columns):
            missing = needed - set(pdf.columns)
            return f"missing_cols:{','.join(sorted(missing))}"
        pdf = pdf[list(needed)].dropna(subset=["safetyreportid", "key", "value"])
        pdf = pdf[pdf["key"].isin(known_keys)]
        if pdf.empty:
            return "empty_after_filter"
        return None  # OK
    except Exception as e:
        return f"read_error:{type(e).__name__}:{str(e)[:80]}"


def _print_openfda_skip_diagnostics(skipped_paths: list, known_keys: list) -> None:
    """Run _diagnose_openfda_file on skipped files and print a summary (when S02_OPENFDA_DIAGNOSE=1)."""
    sample = skipped_paths[:50]  # cap to avoid slowdown
    reasons: dict[str, list[str]] = {}
    for fp in sample:
        reason = _diagnose_openfda_file(Path(fp), known_keys)
        key = reason or "ok"
        reasons.setdefault(key, []).append(Path(fp).name)
    print("  [S02_OPENFDA_DIAGNOSE] Sample of skipped files:", flush=True)
    for reason, names in sorted(reasons.items()):
        print(f"    {reason}: {len(names)} file(s) e.g. {names[:3]}", flush=True)
    if len(skipped_paths) > len(sample):
        print(f"    (showing first {len(sample)} of {len(skipped_paths)} skipped)", flush=True)


def _process_openfda_file_to_shard(
    file_path: Path,
    shard_tmp: Path,
    shard_out: Path,
    known_keys: list,
) -> bool:
    """Read CSV with Polars, aggregate, and write shard parquet."""
    agg = _read_and_aggregate_openfda_file(Path(file_path), known_keys)
    if agg is None:
        return False
    try:
        shard_tmp.parent.mkdir(parents=True, exist_ok=True)
        agg.write_parquet(str(shard_tmp), compression="zstd")
        shard_tmp.rename(shard_out)
        return True
    except Exception:
        return False


def _enumerate_csvs(folder: Path) -> Sequence[str]:
    files: set[Path] = set()
    for pattern in _PATTERNS:
        files.update(folder.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No CSV files found in {folder}")
    file_list = [str(path) for path in sorted(files)]
    print(f"    found {len(file_list)} source files in {folder.name}/", flush=True)
    return file_list


# Per-folder header cache: folder path → {file_path: frozenset of column names}
# Avoids re-reading headers when _scan_csv is called multiple times for the same folder.
_FOLDER_HEADERS: dict[str, dict[str, frozenset]] = {}


def _read_one_header(fp: str) -> tuple[str, frozenset]:
    """Read CSV header for a single file. Runs in parallel worker."""
    try:
        sample = pl.read_csv(fp, has_header=True, n_rows=0, null_values=_NULLS)
        return fp, frozenset(sample.columns)
    except Exception:
        return fp, frozenset()


def _get_file_headers(folder: Path, files: Sequence[str]) -> dict[str, frozenset]:
    """Return cached {fp: frozenset(columns)} for all files in folder (parallel scan)."""
    key = str(folder)
    if key not in _FOLDER_HEADERS:
        n = len(files)
        n_workers = min(16, max(1, n), cpu_count())
        print(f"  [{_ts()}] Scanning headers for {folder.name}/ ({n} files, workers={n_workers}) ...", flush=True)
        t0 = time.time()
        results = Parallel(n_jobs=n_workers, backend="loky")(
            delayed(_read_one_header)(fp) for fp in files
        )
        _FOLDER_HEADERS[key] = dict(results)  # type: ignore[arg-type]
        print(f"  [{_ts()}] Header scan done in {time.time() - t0:.1f}s (cached)", flush=True)
    else:
        print(f"  [{_ts()}] Using cached headers for {folder.name}/ ({len(files)} files)", flush=True)
    return _FOLDER_HEADERS[key]


def _scan_csv(
    folder: Path,
    columns: Iterable[str],
    dtypes: Mapping[str, pl.PolarsDataType] | None = None,
) -> pl.LazyFrame:
    files = _enumerate_csvs(folder)
    required = list(columns)
    dtype_map = dict(dtypes or {})
    scan_kw = dict(has_header=True, infer_schema_length=0, null_values=_NULLS, ignore_errors=True)

    headers = _get_file_headers(folder, files)

    frames = []
    for fp in files:
        avail = headers.get(fp, frozenset())
        overrides = {c: dtype_map[c] for c in avail if c in dtype_map}
        try:
            lf = pl.scan_csv(fp, schema_overrides=overrides, **scan_kw)
        except TypeError:
            lf = pl.scan_csv(fp, dtypes=overrides, **scan_kw)
        for c in required:
            if c not in avail:
                lf = lf.with_columns(pl.lit(None, dtype=dtype_map.get(c, pl.Utf8)).alias(c))
        lf = lf.select(required)
        frames.append(lf)

    return pl.concat(frames, how="diagonal_relaxed")


def _ts() -> str:
    """Return current timestamp string for inline logging."""
    return time.strftime("%H:%M:%S")


def _write(lf: pl.LazyFrame, path: Path) -> int:
    t0 = time.time()
    print(f"  [{_ts()}] → Writing {path.name} ...", flush=True)
    path.parent.mkdir(parents=True, exist_ok=True)
    lf.sink_parquet(path, compression="zstd")
    rows = int(
        pl.scan_parquet(path)
        .select(pl.len().alias("rows"))
        .collect(streaming=True)["rows"][0]
    )
    print(f"  [{_ts()}] ✅ {path.name}: {rows:,} rows  ({time.time() - t0:.1f}s)", flush=True)
    return rows


def _build_report_tables(root: Path, out_dir: Path) -> Dict[str, Dict[str, object]]:
    report_dir = root / "report"

    date_lf = (
        _scan_csv(
            report_dir,
            ["safetyreportid", "receiptdate", "receivedate", "transmissiondate"],
            {"safetyreportid": pl.Utf8},
        )
        .select(
            pl.col("safetyreportid").cast(pl.Utf8),
            pl.col("receiptdate").cast(pl.Utf8).alias("mostrecent_receive_date"),
            pl.col("receivedate").cast(pl.Utf8).alias("receive_date"),
            pl.col("transmissiondate").cast(pl.Utf8).alias("lastupdate_date"),
        )
        .filter(pl.col("safetyreportid").is_not_null())
        .group_by("safetyreportid")
        .agg(
            pl.col("mostrecent_receive_date").max().alias("mostrecent_receive_date"),
            pl.col("receive_date").max().alias("receive_date"),
            pl.col("lastupdate_date").max().alias("lastupdate_date"),
        )
    )
    date_path = out_dir / "report.parquet"
    date_rows = _write(date_lf, date_path)

    serious_map = {
        "serious": "serious",
        "seriousnesscongenitalanomali": "congenital_anomali",
        "seriousnessdeath": "death",
        "seriousnessdisabling": "disabling",
        "seriousnesshospitalization": "hospitalization",
        "seriousnesslifethreatening": "life_threatening",
        "seriousnessother": "other",
    }
    serious_lf = (
        _scan_csv(
            report_dir,
            ["safetyreportid", *serious_map.keys()],
            {"safetyreportid": pl.Utf8},
        )
        .with_columns(
            pl.col("safetyreportid").cast(pl.Utf8),
            *[
                pl.col(src).cast(pl.Utf8, strict=False).alias(dst)
                for src, dst in serious_map.items()
            ],
        )
        .filter(pl.col("safetyreportid").is_not_null())
        .group_by("safetyreportid")
        .agg(
            *[
                pl.col(dst)
                .drop_nulls()
                .first()
                .alias(dst)
                for dst in serious_map.values()
            ],
        )
    )
    serious_path = out_dir / "report_serious.parquet"
    serious_rows = _write(serious_lf, serious_path)

    reporter_map = {
        "companynumb": "reporter_company",
        "primarysource.qualification": "reporter_qualification",
        "primarysource.reportercountry": "reporter_country",
    }
    reporter_lf = (
        _scan_csv(
            report_dir,
            ["safetyreportid", *reporter_map.keys()],
            {"safetyreportid": pl.Utf8},
        )
        .select(
            pl.col("safetyreportid").cast(pl.Utf8),
            *[
                pl.col(src).cast(pl.Utf8, strict=False).alias(dst)
                for src, dst in reporter_map.items()
            ],
        )
        .filter(pl.col("safetyreportid").is_not_null())
        .group_by("safetyreportid")
        .agg(
            *[
                pl.col(dst).drop_nulls().first().alias(dst)
                for dst in reporter_map.values()
            ]
        )
    )
    reporter_path = out_dir / "reporter.parquet"
    reporter_rows = _write(reporter_lf, reporter_path)

    logger.info(
        "[S02-stream] report={} report_serious={} reporter={}",
        date_rows,
        serious_rows,
        reporter_rows,
    )

    return {
        "report": {"rows": date_rows, "path": str(date_path)},
        "report_serious": {"rows": serious_rows, "path": str(serious_path)},
        "reporter": {"rows": reporter_rows, "path": str(reporter_path)},
    }


def _build_patient_table(root: Path, out_dir: Path) -> Dict[str, object]:
    patient_dir = root / "patient"
    patient_map = {
        "patient.patientonsetage": ("patient_onsetage", pl.Float64),
        "patient.patientonsetageunit": ("patient_onsetageunit", pl.Utf8),
        "master_age": ("patient_custom_master_age", pl.Float64),
        "patient.patientsex": ("patient_sex", pl.Utf8),
        "patient.patientweight": ("patient_weight", pl.Float64),
    }
    patient_lf = (
        _scan_csv(
            patient_dir,
            ["safetyreportid", *patient_map.keys()],
            {"safetyreportid": pl.Utf8},
        )
        .select(
            pl.col("safetyreportid").cast(pl.Utf8),
            *[
                pl.col(src).cast(dtype, strict=False).alias(alias)
                for src, (alias, dtype) in patient_map.items()
            ],
        )
        .filter(pl.col("safetyreportid").is_not_null())
        .group_by("safetyreportid")
        .agg(
            *[
                pl.col(alias).drop_nulls().first().alias(alias)
                for alias, _ in patient_map.values()
            ]
        )
    )
    patient_path = out_dir / "patient.parquet"
    patient_rows = _write(patient_lf, patient_path)
    logger.info("[S02-stream] patient rows={}", patient_rows)
    return {"rows": patient_rows, "path": str(patient_path)}


def _build_drug_table(root: Path, out_dir: Path) -> Dict[str, object]:
    """Build the original drugcharacteristics.parquet (backward-compatible).

    Unit: report-level drug rows (safetyreportid × medicinal_product, de-duplicated).
    This table is used by S03 and downstream stages — do NOT change its schema.
    """
    drug_dir = root / "patient_drug"
    drug_map = {
        "medicinalproduct": "medicinal_product",
        "drugcharacterization": "drug_characterization",
        "drugadministrationroute": "drug_administration",
        "drugindication": "drug_indication",
    }
    drug_lf = (
        _scan_csv(
            drug_dir,
            ["safetyreportid", *drug_map.keys()],
            {"safetyreportid": pl.Utf8},
        )
        .select(
            pl.col("safetyreportid").cast(pl.Utf8),
            *[
                pl.col(src).cast(pl.Utf8, strict=False).alias(dst)
                for src, dst in drug_map.items()
            ],
        )
        .filter(pl.col("safetyreportid").is_not_null())
        .filter(pl.col("medicinal_product").is_not_null())
        .unique()
    )
    drug_path = out_dir / "drugcharacteristics.parquet"
    drug_rows = _write(drug_lf, drug_path)
    logger.info("[S02-stream] drugcharacteristics rows={}", drug_rows)
    return {"rows": drug_rows, "path": str(drug_path)}


# ---------------------------------------------------------------------------
# New drug-level extended outputs for advanced mapping
# ---------------------------------------------------------------------------

# All source column names from patient_drug CSVs (from S01) that we want to
# preserve in the extended table, mapped to output column names.
# Unit: 1 row = 1 (safetyreportid, entry) — do NOT unique() this table.
_DRUG_EXTENDED_SRC_TO_DST: Dict[str, str] = {
    "medicinalproduct":               "medicinal_product",
    "drugcharacterization":           "drug_characterization",
    "drugadministrationroute":        "drug_administration",
    "drugindication":                 "drug_indication",
    "drugdosageform":                 "drug_dosage_form",
    "drugauthorizationnumb":          "drug_authorization_number",
    "drugbatchnumb":                  "drug_batch_number",
    "drugstructuredosagenumb":        "drug_structured_dosage_num",
    "drugstructuredosageunit":        "drug_structured_dosage_unit",
    "drugintervaldosageunitnumb":     "drug_interval_dosage_unit_num",
    "drugintervaldosagedefinition":   "drug_interval_dosage_definition",
    "drugtreatmentduration":          "drug_treatment_duration",
    "drugtreatmentdurationunit":      "drug_treatment_duration_unit",
    "drugseparatedosagenumb":         "drug_separate_dosage_num",
    "actiondrug":                     "action_drug",
}

# openfda keys we care about — defines the columns in drug_openfda_wide.parquet
OPENFDA_KNOWN_KEYS: list = [
    "rxcui",
    "generic_name",
    "brand_name",
    "substance_name",
    "product_ndc",
    "package_ndc",
    "application_number",
    "manufacturer_name",
    "route",
    "spl_set_id",
    "nui",
    "pharm_class_cs",
    "pharm_class_epc",
    "pharm_class_moa",
    "pharm_class_pe",
    "product_type",
    "unii",
]


def _extract_active_substance_name_expr(raw_col: str = "activesubstance_raw") -> pl.Expr:
    """Parse activesubstancename from raw JSON/dict string. Returns null on failure."""
    raw = pl.col(raw_col).cast(pl.Utf8, strict=False)
    pattern = r"""['"]activesubstancename['"]\s*:\s*['"]([^'"]+)['"]"""
    parsed = raw.str.extract(pattern, 1).str.strip_chars().str.replace_all(r"\s{2,}", " ")
    return pl.when(parsed.is_not_null() & (parsed.str.len_chars() > 0)).then(parsed).otherwise(pl.lit(None, dtype=pl.Utf8))


def _build_drug_extended_table(root: Path, out_dir: Path) -> Dict[str, object]:
    """Build drugcharacteristics_extended.parquet. 1 row = 1 (safetyreportid, entry)."""
    drug_dir = root / "patient_drug"
    src_cols = list(_DRUG_EXTENDED_SRC_TO_DST.keys())
    all_required = ["safetyreportid", "entry", "activesubstance", "activesubstance_name"] + src_cols

    lf = _scan_csv(drug_dir, all_required, {"safetyreportid": pl.Utf8, "entry": pl.Int64})
    lf = lf.with_columns(
        pl.col("safetyreportid").cast(pl.Utf8, strict=False),
        pl.col("entry").cast(pl.Int64, strict=False),
        pl.col("activesubstance").cast(pl.Utf8, strict=False).alias("activesubstance_raw"),
    )
    lf = lf.with_columns(
        pl.coalesce([
            pl.col("activesubstance_name").cast(pl.Utf8, strict=False),
            _extract_active_substance_name_expr("activesubstance_raw"),
        ]).alias("active_substance_name")
    ).drop("activesubstance", "activesubstance_name")

    select_exprs = [
        pl.col("safetyreportid"),
        pl.col("entry"),
        *[
            pl.col(src).cast(pl.Utf8, strict=False).alias(dst)
            for src, dst in _DRUG_EXTENDED_SRC_TO_DST.items()
        ],
        pl.col("activesubstance_raw"),    # raw dict string — for audit/debug
        pl.col("active_substance_name"),  # parsed clean name — for mapping
    ]
    lf = (
        lf
        .select(select_exprs)
        .filter(pl.col("safetyreportid").is_not_null())
        # Keep only the first occurrence of each (safetyreportid, entry) pair.
        # FAERS source CSVs sometimes contain duplicate drug entries for the same
        # report+position (quarter overlap / re-submission artifacts). Dedup here
        # ensures the PK constraint holds and prevents row-count inflation in all
        # downstream tables that LEFT JOIN on (safetyreportid, entry).
        .unique(subset=["safetyreportid", "entry"], keep="first", maintain_order=False)
    )

    out_path = out_dir / "drugcharacteristics_extended.parquet"
    rows = _write(lf, out_path)

    # Coverage stats
    stats = pl.scan_parquet(str(out_path)).select(
        pl.col("activesubstance_raw").is_not_null().sum().alias("raw_nn"),
        pl.col("active_substance_name").is_not_null().sum().alias("parsed_nn"),
        pl.len().alias("total"),
    ).collect()
    raw_nn, parsed_nn, total = int(stats["raw_nn"][0]), int(stats["parsed_nn"][0]), int(stats["total"][0])
    cov_raw = raw_nn / total * 100 if total else 0
    cov_parsed = parsed_nn / total * 100 if total else 0
    print(f"  activesubstance_raw: {raw_nn:,}/{total:,} ({cov_raw:.1f}%)", flush=True)
    rate = f" [parse: {parsed_nn/raw_nn*100:.1f}% of raw]" if raw_nn else ""
    print(f"  active_substance_name: {parsed_nn:,}/{total:,} ({cov_parsed:.1f}%){rate}", flush=True)
    logger.info(
        "[S02-stream] drugcharacteristics_extended rows={}  "
        "activesubstance_raw={} ({:.1f}%)  active_substance_name={} ({:.1f}%)",
        rows, raw_nn, cov_raw, parsed_nn, cov_parsed,
    )
    return {
        "rows": rows,
        "path": str(out_path),
        "activesubstance_raw_count":    raw_nn,
        "active_substance_name_count":  parsed_nn,
        "active_substance_name_coverage_pct": round(cov_parsed, 2),
    }


def _openfda_build_shards(
    openfda_dir: Path,
    known_keys: list,
    tmp_dir: Path,
    n_jobs: int,
    batch_size: int = 100,
) -> tuple[list[str], list[str]]:
    """Read openfda CSVs in parallel, write per-file shard parquets, return (shard_files, skipped_paths).

    Shards are reused across runs: if a shard already exists it is not re-read.
    Parallel reading via joblib gives ~n_jobs× speedup over sequential.
    """
    tmp_dir.mkdir(parents=True, exist_ok=True)
    for stale in tmp_dir.glob("*.parquet.tmp"):
        stale.unlink(missing_ok=True)

    files = _enumerate_csvs(openfda_dir)

    # Partition into already-done and pending
    pending, already_done = [], []
    for fp in files:
        shard_out = tmp_dir / (Path(fp).stem + ".parquet")
        if shard_out.exists():
            already_done.append(str(shard_out))
        else:
            pending.append(fp)

    if already_done:
        print(f"  → Reusing {len(already_done)} cached shards, processing {len(pending)} new files...", flush=True)
    else:
        print(f"  → Processing {len(pending)} files (n_jobs={n_jobs})...", flush=True)

    skipped_paths: list[str] = []
    new_shards: list[str] = []

    if pending:
        shard_outs = [str(tmp_dir / (Path(fp).stem + ".parquet")) for fp in pending]
        shard_tmps = [str(tmp_dir / (Path(fp).stem + ".parquet.tmp")) for fp in pending]

        print(f"  [{_ts()}] Processing {len(pending)} files with {n_jobs} workers ...", flush=True)
        results = Parallel(n_jobs=n_jobs, backend="loky", verbose=0)(
            delayed(_process_openfda_file_to_shard)(
                Path(fp), Path(stmp), Path(sout), known_keys
            )
            for fp, stmp, sout in zip(pending, shard_tmps, shard_outs)
        )
        for fp, sout, ok in zip(pending, shard_outs, results):
            if ok:
                new_shards.append(sout)
            else:
                skipped_paths.append(fp)

    if skipped_paths:
        print(f"  ⚠  Skipped {len(skipped_paths)} file(s) due to read errors.", flush=True)
        if os.environ.get("S02_OPENFDA_DIAGNOSE"):
            _print_openfda_skip_diagnostics(skipped_paths, known_keys)

    all_shards = sorted(set(already_done + new_shards))
    print(
        f"  → Shards: {len(new_shards)} new, {len(already_done)} reused, {len(skipped_paths)} skipped",
        flush=True,
    )
    return all_shards, skipped_paths


def _openfda_collect_per_file(openfda_dir: Path, known_keys: list, tmp_dir: Path) -> pl.LazyFrame:
    """Read openfda CSVs (sequential fallback) → aggregate per file → write shards."""
    n_jobs = int(os.environ.get("S02_OPENFDA_JOBS", str(min(cpu_count(), 16))))
    n_jobs = min(n_jobs, 64)
    shard_files, _ = _openfda_build_shards(openfda_dir, known_keys, tmp_dir, n_jobs=n_jobs)

    if not shard_files:
        return pl.DataFrame(
            schema={"safetyreportid": pl.Utf8, "entry": pl.Int64, "key": pl.Utf8, "values": pl.List(pl.Utf8)}
        ).lazy()

    return (
        pl.scan_parquet(shard_files)
        .group_by(["safetyreportid", "entry", "key"])
        .agg(pl.col("values").flatten().drop_nulls().alias("values"))
    )


def _log_mem() -> str:
    """Return RSS memory usage string (requires psutil; skipped silently if unavailable)."""
    if not _HAS_PSUTIL or _PSUTIL_PROC is None:
        return ""
    try:
        rss = _PSUTIL_PROC.memory_info().rss / 1024 ** 3
        return f"  [mem {rss:.1f}GB]"
    except Exception:
        return ""


def _merge_one_batch(args: tuple) -> bool:
    """Merge one batch of shard parquets into a single intermediate parquet.

    Defined at module level so joblib can serialize it without closure issues.
    """
    batch_idx, batch_paths, out_p = args
    try:
        (
            pl.scan_parquet(batch_paths)
            .group_by(["safetyreportid", "entry", "key"])
            .agg(pl.col("values").flatten().drop_nulls().unique().alias("values"))
            .sink_parquet(out_p, compression="zstd")
        )
        return True
    except Exception as exc:
        print(f"  [batch {batch_idx}] ERROR: {exc}", flush=True)
        return False


def _run_duckdb_query(sql: str, memory_gb: int, n_threads: int, tmp_dir: Path) -> None:
    """Execute a single DuckDB SQL statement with configured memory/threads."""
    import duckdb

    con = duckdb.connect(":memory:")
    try:
        con.execute(f"SET memory_limit='{memory_gb}GB'")
        con.execute(f"SET threads={n_threads}")
        con.execute(f"SET temp_directory='{tmp_dir}'")
        con.execute(sql)
    finally:
        con.close()


def _build_wide_duckdb(
    interm_files: list,
    known_keys: list,
    out_path: Path,
    tmp_dir: Path,
    memory_gb: int = 80,
    n_threads: int = 20,
    n_partitions: int = 8,
) -> int:
    """Hash-partitioned DuckDB GROUP BY over long-format intermediates → wide parquet.

    Root cause of previous OOM: one-pass GROUP BY on 55GB → 54M rows × 17 list cols
    needs ~400-500GB (input decompressed ~275GB + hash table ~150GB+).

    Fix: split into N hash-partitions using `WHERE hash(safetyreportid) % N = P`.
    Each query handles only 54M/N rows in its hash table → peak RAM per query:
      - Input streaming:  ~55GB (DuckDB reads+decompresses on-the-fly)
      - Hash table:       54M/N rows × 17 list cols  (N=8 → 6.75M rows, ~30-50GB)
      - Total peak:       ~80-100GB  (safe with 120GB limit)
    Trade-off: reads 55GB × N from NFS total, but no OOM.
    """
    t_total = time.time()
    n_partitions = max(1, n_partitions)
    files_sql = "[" + ", ".join(f"'{f}'" for f in interm_files) + "]"
    agg_cols = ",\n  ".join(
        f"list_distinct(flatten(list(values) FILTER (WHERE key = '{k}'))) AS openfda_{k}"
        for k in known_keys
    )

    if n_partitions == 1:
        # Single-pass (only safe on very high-memory nodes)
        sql = f"""
        COPY (
            SELECT safetyreportid, entry,
                   {agg_cols}
            FROM read_parquet({files_sql})
            GROUP BY safetyreportid, entry
        ) TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
        print(
            f"  [{_ts()}] DuckDB: 1-pass GROUP BY "
            f"({len(known_keys)} cols, mem={memory_gb}GB, threads={n_threads}) ...",
            flush=True,
        )
        _run_duckdb_query(sql, memory_gb, n_threads, tmp_dir)
    else:
        # Hash-partitioned: N separate queries, each reads 55GB but only holds
        # 54M/N rows in its hash table → bounded peak RAM.
        parts_dir = tmp_dir / "_duck_parts"
        parts_dir.mkdir(parents=True, exist_ok=True)

        print(
            f"  [{_ts()}] DuckDB: {n_partitions} hash-partitions × GROUP BY "
            f"({len(known_keys)} cols, mem={memory_gb}GB, threads={n_threads}) ...",
            flush=True,
        )
        part_files: list[str] = []
        for p in range(n_partitions):
            part_out = str(parts_dir / f"duck_{p:03d}.parquet")
            part_files.append(part_out)
            if Path(part_out).exists():
                part_rows = int(
                    pl.scan_parquet(part_out).select(pl.len().alias("n")).collect()["n"][0]
                )
                print(f"  [{_ts()}]   Partition {p + 1}/{n_partitions}: {part_rows:,} rows (reused)", flush=True)
                continue
            t_p = time.time()
            sql = f"""
            COPY (
                SELECT safetyreportid, entry,
                       {agg_cols}
                FROM read_parquet({files_sql})
                WHERE hash(safetyreportid) % {n_partitions} = {p}
                GROUP BY safetyreportid, entry
            ) TO '{part_out}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """
            _run_duckdb_query(sql, memory_gb, n_threads, tmp_dir)
            part_rows = int(
                pl.scan_parquet(part_out).select(pl.len().alias("n")).collect()["n"][0]
            )
            print(
                f"  [{_ts()}]   Partition {p + 1}/{n_partitions}: "
                f"{part_rows:,} rows in {time.time() - t_p:.0f}s",
                flush=True,
            )
            gc.collect()

        # Concatenate all partition files → final output
        print(f"  [{_ts()}] Concatenating {n_partitions} DuckDB partitions ...", flush=True)
        pl.scan_parquet(part_files).sink_parquet(str(out_path), compression="zstd")
        for f in part_files:
            Path(f).unlink(missing_ok=True)
        try:
            parts_dir.rmdir()
        except OSError:
            pass

    rows = int(pl.scan_parquet(str(out_path)).select(pl.len().alias("n")).collect()["n"][0])
    print(f"  [{_ts()}] DuckDB done: {rows:,} rows total in {time.time() - t_total:.0f}s", flush=True)
    return rows


def _pivot_one_partition(args: tuple) -> int:
    """Pivot one hash-partition of openfda data. Runs in a joblib worker process."""
    p, part_data_dir_str, wide_dir_str, known_keys_tuple = args
    import gc as _gc
    import polars as _pl
    from pathlib import Path as _Path

    known_keys = list(known_keys_tuple)
    wide_out = _Path(wide_dir_str) / f"wide_{p:04d}.parquet"
    if wide_out.exists():
        return int(_pl.scan_parquet(str(wide_out)).select(_pl.len().alias("n")).collect()["n"][0])

    part_files = sorted(_Path(part_data_dir_str).glob(f"p{p:04d}_*.parquet"))
    if not part_files:
        return 0

    df = _pl.concat([_pl.read_parquet(str(f)) for f in part_files], rechunk=True)
    df = (
        df.group_by(["safetyreportid", "entry", "key"])
        .agg(_pl.col("values").flatten().drop_nulls().unique())
    )
    if df.is_empty():
        return 0

    wide = df.pivot(on="key", index=["safetyreportid", "entry"], values="values", aggregate_function=None)
    del df
    _gc.collect()

    wide = wide.rename({k: f"openfda_{k}" for k in known_keys if k in wide.columns})
    for k in known_keys:
        if f"openfda_{k}" not in wide.columns:
            wide = wide.with_columns(_pl.lit(None, dtype=_pl.List(_pl.Utf8)).alias(f"openfda_{k}"))
    wide = wide.select(["safetyreportid", "entry"] + [f"openfda_{k}" for k in known_keys])
    wide.write_parquet(str(wide_out), compression="zstd")
    h = wide.height
    del wide
    _gc.collect()
    return h


def _build_wide_polars_partitioned(
    interm_files: list,
    known_keys: list,
    out_path: Path,
    tmp_dir: Path,
    n_partitions: int = 64,
    n_jobs: int = 4,
) -> int:
    """Two-pass Polars fallback when DuckDB is unavailable.

    Pass 1: Read each of the 15 intermediates once and split into N partition files
            by hash(safetyreportid) % N.  Total I/O: 55GB read + 55GB write.
    Pass 2: Pivot each partition (group_by + pivot) in parallel.
            Peak RAM per worker: ~55GB / N (~860MB for N=64).
    """
    part_data_dir = tmp_dir / "_part_data"
    wide_dir = tmp_dir / "_wide_parts"
    part_data_dir.mkdir(parents=True, exist_ok=True)
    wide_dir.mkdir(parents=True, exist_ok=True)

    # Pass 1: Partition each intermediate by hash(safetyreportid) % N
    print(
        f"  [{_ts()}] Polars pass 1: partitioning {len(interm_files)} intermediates"
        f" → {n_partitions} buckets ...{_log_mem()}",
        flush=True,
    )
    for j, interm in enumerate(interm_files):
        sentinel = part_data_dir / f"_done_{j:02d}"
        if sentinel.exists():
            continue
        df = pl.read_parquet(interm).with_columns(
            pl.col("safetyreportid").hash().mod(n_partitions).alias("_p")
        )
        for p in range(n_partitions):
            chunk = df.filter(pl.col("_p") == p).drop("_p")
            if chunk.height > 0:
                chunk.write_parquet(part_data_dir / f"p{p:04d}_{j:02d}.parquet", compression="zstd")
        del df
        gc.collect()
        sentinel.touch()
        if (j + 1) % 3 == 0 or j == len(interm_files) - 1:
            print(f"  [{_ts()}]   Partitioned {j + 1}/{len(interm_files)}{_log_mem()}", flush=True)

    # Pass 2: Pivot each partition in parallel
    n_par = min(n_jobs, n_partitions, cpu_count())
    print(
        f"  [{_ts()}] Polars pass 2: pivoting {n_partitions} partitions (jobs={n_par}) ...{_log_mem()}",
        flush=True,
    )
    results = Parallel(n_jobs=n_par, backend="loky")(
        delayed(_pivot_one_partition)(
            (p, str(part_data_dir), str(wide_dir), tuple(known_keys))
        )
        for p in range(n_partitions)
    )
    total = sum(r for r in results if r is not None)
    print(f"  [{_ts()}]   {total:,} rows across {n_partitions} partitions{_log_mem()}", flush=True)

    wide_parts = sorted(wide_dir.glob("wide_*.parquet"))
    pl.scan_parquet([str(p) for p in wide_parts]).sink_parquet(str(out_path), compression="zstd")
    return total


def _build_openfda_wide_high_memory(
    openfda_dir: Path,
    out_path: Path,
    known_keys: list,
    tmp_dir: Path,
) -> Dict[str, object]:
    """Parallel read → per-file shard cache → batch merge → wide table.

    Phases:
      1. Build per-file shards (parallel, cached).
      2. Batch-merge shards → 15 intermediate parquets (cached).
      3. Wide table: DuckDB one-pass GROUP BY (fast, ~10-20 min) or
         Polars two-pass partition-pivot fallback.
    """
    t_start = time.time()
    n_jobs = int(os.environ.get("S02_OPENFDA_JOBS", str(min(cpu_count(), 16))))
    n_jobs = min(n_jobs, 64)

    # ── Phase 1: Build per-file shards (reuses existing shards) ──────────────
    print(f"  [{_ts()}] Building shards (n_jobs={n_jobs}, tmp={tmp_dir.name}) ...", flush=True)
    shard_files, skipped_paths = _openfda_build_shards(
        openfda_dir, known_keys, tmp_dir, n_jobs=n_jobs
    )
    skipped = len(skipped_paths)
    print(f"  [{_ts()}] Shard phase done in {time.time() - t_start:.0f}s — {len(shard_files)} shards{_log_mem()}", flush=True)

    if not shard_files:
        pl.DataFrame({
            "safetyreportid": pl.Series([], dtype=pl.Utf8),
            "entry":          pl.Series([], dtype=pl.Int64),
        }).write_parquet(str(out_path), compression="zstd")
        return {"rows": 0, "path": str(out_path), "keys_found": [], "skipped_files": skipped}

    # ── Phase 2: Batch merge shards → intermediate parquets ──────────────────
    MERGE_BATCH = int(os.environ.get("S02_MERGE_BATCH", "120"))
    parallel_batches = int(os.environ.get("S02_MERGE_PARALLEL_BATCHES", "4"))
    intermediates_dir = tmp_dir / "_merge_intermediates"
    intermediates_dir.mkdir(parents=True, exist_ok=True)

    n_batches = (len(shard_files) + MERGE_BATCH - 1) // MERGE_BATCH
    # Known complete counts for 1710 shards: 22 (batch=80), 15 (batch=120)
    n_batches_legacy_80 = (len(shard_files) + 79) // 80
    complete_counts = {n_batches, n_batches_legacy_80}

    # Resume: skip batch merge if we have a complete set (current or legacy batch size)
    existing_intermediates = sorted(intermediates_dir.glob("merge_batch_*.parquet"))
    if len(existing_intermediates) in complete_counts:
        print(
            f"  [{_ts()}] RESUME: {len(existing_intermediates)} intermediates exist — skipping batch merge{_log_mem()}",
            flush=True,
        )
        interm_files = [str(p) for p in existing_intermediates]
    else:
        # Remove stale/partial intermediates before re-running
        for stale in existing_intermediates:
            stale.unlink(missing_ok=True)

        batch_tasks: list[tuple] = []
        for i in range(0, len(shard_files), MERGE_BATCH):
            batch = shard_files[i : i + MERGE_BATCH]
            batch_idx = i // MERGE_BATCH
            interm_path = str(intermediates_dir / f"merge_batch_{batch_idx:04d}.parquet")
            batch_tasks.append((batch_idx, batch, interm_path))

        t_merge = time.time()
        print(
            f"  [{_ts()}] Merging {len(shard_files)} shards in {n_batches} batches "
            f"(batch_size={MERGE_BATCH}, parallel={parallel_batches}) ...{_log_mem()}",
            flush=True,
        )

        if parallel_batches <= 1:
            for task in batch_tasks:
                _merge_one_batch(task)
                batch_idx = task[0]
                if (batch_idx + 1) % 5 == 0 or batch_idx == n_batches - 1:
                    print(f"  [{_ts()}]   Batch {batch_idx + 1}/{n_batches} done{_log_mem()}", flush=True)
        else:
            # Single Parallel call — one loky pool for all batches, no repeated spawn/teardown
            n_parallel = min(parallel_batches, n_batches, cpu_count())
            Parallel(n_jobs=n_parallel, backend="loky", verbose=0)(
                delayed(_merge_one_batch)(t) for t in batch_tasks
            )
            print(f"  [{_ts()}]   All {n_batches} batches done in {time.time() - t_merge:.0f}s{_log_mem()}", flush=True)

        interm_files = sorted(str(p) for p in intermediates_dir.glob("merge_batch_*.parquet"))
        print(f"  [{_ts()}] Batch merge done: {len(interm_files)} intermediates{_log_mem()}", flush=True)

    # ── Phase 3: Build wide table ─────────────────────────────────────────────
    # Primary:  DuckDB one-pass GROUP BY over intermediates (10-20 min, ~2GB RAM).
    # Fallback: Polars two-pass partition-pivot (pre-partition once, pivot in parallel).
    import shutil as _shutil

    t_wide = time.time()
    wide_tmp_dir = tmp_dir / "_wide_build"
    wide_tmp_dir.mkdir(parents=True, exist_ok=True)

    n_threads = int(os.environ.get("S02_THREADS", str(min(cpu_count(), 20))))
    memory_gb = int(os.environ.get("S02_MEMORY_GB", "80"))

    # Clean up legacy _key_columns from previous attempts (up to ~30GB saved)
    _key_legacy = tmp_dir / "_key_columns"
    if _key_legacy.exists():
        try:
            _sz = sum(f.stat().st_size for f in _key_legacy.rglob("*") if f.is_file()) // 1024**3
            print(f"  [{_ts()}] Cleaning legacy _key_columns ({_sz}GB) ...", flush=True)
        except Exception:
            pass
        _shutil.rmtree(str(_key_legacy), ignore_errors=True)

    _has_duckdb = False
    try:
        import duckdb as _duckdb  # noqa: F401
        _has_duckdb = True
    except ImportError:
        pass

    print(
        f"  [{_ts()}] Building wide table — "
        f"engine={'duckdb' if _has_duckdb else 'polars'}, "
        f"threads={n_threads}, mem={memory_gb}GB, "
        f"intermediates={len(interm_files)}, keys={len(known_keys)}{_log_mem()}",
        flush=True,
    )

    if _has_duckdb:
        # S02_DUCK_PARTITIONS=8: splits 54M rows into 8 hash-buckets
        # Each bucket needs ~55GB (input streaming) + 54M/8=6.75M rows hash table
        # → peak RAM per query ~80-100GB, well within 400GB limit.
        n_duck_part = int(os.environ.get("S02_DUCK_PARTITIONS", "8"))
        n_duck_part = max(1, min(n_duck_part, 64))
        rows = _build_wide_duckdb(
            interm_files, known_keys, out_path, wide_tmp_dir,
            memory_gb=memory_gb, n_threads=n_threads, n_partitions=n_duck_part,
        )
    else:
        n_part = int(os.environ.get("S02_WIDE_PARTITIONS", "64"))
        n_part = max(8, min(n_part, 256))
        n_wide_jobs = int(os.environ.get("S02_WIDE_JOBS", str(min(4, cpu_count()))))
        rows = _build_wide_polars_partitioned(
            interm_files, known_keys, out_path, wide_tmp_dir,
            n_partitions=n_part, n_jobs=n_wide_jobs,
        )

    _shutil.rmtree(str(wide_tmp_dir), ignore_errors=True)

    keys_found = list(known_keys)
    elapsed_wide = time.time() - t_wide
    print(
        f"  [{_ts()}] ✅ {out_path.name}: {rows:,} rows  "
        f"(phase3: {elapsed_wide:.0f}s / total {time.time() - t_start:.0f}s){_log_mem()}",
        flush=True,
    )
    logger.info("[S02-stream] drug_openfda_wide rows={}  keys={}", rows, keys_found)
    return {
        "rows": rows,
        "path": str(out_path),
        "keys_found": keys_found,
        "skipped_files": skipped,
    }


def _build_drug_openfda_wide(root: Path, out_dir: Path, ctx: PipelineContext) -> Dict[str, object]:
    """Build drug_openfda_wide.parquet. 1 row = 1 (safetyreportid, entry).

    Both high-memory and low-memory config values use the same unified path:
    parallel Polars reading → per-file shard cache → scan_parquet → pivot.
    """
    openfda_dir = root / "patient_drug_openfda"
    out_path = out_dir / "drug_openfda_wide.parquet"
    tmp_dir  = out_dir / "_tmp_openfda_shards"

    _EMPTY_PLACEHOLDER = pl.DataFrame({
        "safetyreportid": pl.Series([], dtype=pl.Utf8),
        "entry":          pl.Series([], dtype=pl.Int64),
    })

    if not openfda_dir.exists() or not list(openfda_dir.glob("*.csv*")):
        logger.warning("[S02-stream] No patient_drug_openfda CSV files found — skipping wide table")
        _EMPTY_PLACEHOLDER.write_parquet(str(out_path), compression="zstd")
        return {"rows": 0, "path": str(out_path), "keys_found": [], "skipped_files": 0}

    return _build_openfda_wide_high_memory(openfda_dir, out_path, OPENFDA_KNOWN_KEYS, tmp_dir)


def _build_drug_mapping_input(out_dir: Path) -> Dict[str, object]:
    """Merge drugcharacteristics_extended + drug_openfda_wide on (safetyreportid, entry)."""
    extended_path = out_dir / "drugcharacteristics_extended.parquet"
    wide_path     = out_dir / "drug_openfda_wide.parquet"
    out_path      = out_dir / "drug_mapping_input.parquet"

    for p in (extended_path, wide_path):
        if not p.exists():
            raise FileNotFoundError(f"[S02] Missing prerequisite for drug_mapping_input: {p}")

    print(f"  [{_ts()}] LEFT JOIN extended ({extended_path.stat().st_size // 1024**2}MB) + wide ({wide_path.stat().st_size // 1024**2}MB) ...", flush=True)
    extended_lf = pl.scan_parquet(str(extended_path))
    wide_lf     = pl.scan_parquet(str(wide_path))

    merged_lf = extended_lf.join(
        wide_lf,
        on=["safetyreportid", "entry"],
        how="left",
    )

    rows = _write(merged_lf, out_path)
    logger.info("[S02-stream] drug_mapping_input rows={}", rows)
    return {"rows": rows, "path": str(out_path)}


def _build_drug_mapping_input_unique(out_dir: Path) -> Dict[str, object]:
    """Deduplicate drug_mapping_input by drug content. Adds mapping_record_id, source_record_count."""
    src_path = out_dir / "drug_mapping_input.parquet"
    out_path = out_dir / "drug_mapping_input_unique.parquet"
    if not src_path.exists():
        raise FileNotFoundError(f"[S02] Missing drug_mapping_input.parquet at {src_path}")

    t0 = time.time()
    src_mb = src_path.stat().st_size // 1024**2
    print(
        f"  [{_ts()}] Deduplicating drug_mapping_input ({src_mb}MB, "
        f"eager load would be ~{src_mb * 5 // 1024}GB in RAM — using streaming){_log_mem()}",
        flush=True,
    )

    # ── Dedup key definitions (same logic as original) ──────────────────────
    # str_cols: cast to varchar, fill null → ""
    # list_cols: take first element, null if empty/absent
    _STR_KEY_COLS  = ["medicinal_product", "drug_administration", "drug_dosage_form",
                      "active_substance_name", "drug_authorization_number"]
    _LIST_KEY_COLS = ["openfda_generic_name", "openfda_brand_name",
                      "openfda_substance_name", "openfda_application_number"]

    _has_duckdb = False
    try:
        import duckdb as _ddb  # noqa: F401
        _has_duckdb = True
    except ImportError:
        pass

    if _has_duckdb:
        # ── DuckDB path: N hash-partitioned window queries ────────────────────
        # Same strategy as _build_wide_duckdb for Phase 3:
        #   hash of all 10 dedup keys % N = P  →  only 74M/N rows per window call
        # Single-pass on 74M rows needed 111.7 GB (> 120 GB limit).
        # With N=8: ~9.25M rows per pass → ~15-25 GB peak per query.
        # All rows with the same drug profile hash to the same partition,
        # so dedup is correct across partitions.
        memory_gb   = int(os.environ.get("S02_MEMORY_GB", "80"))
        n_threads   = int(os.environ.get("S02_THREADS", str(min(cpu_count(), 20))))
        n_parts     = int(os.environ.get("S02_DEDUP_PARTITIONS", "8"))

        import duckdb

        duck_dir = out_path.parent / "_tmp_dedup_parts"
        duck_dir.mkdir(exist_ok=True)

        n_keys = len(_STR_KEY_COLS) + len(_LIST_KEY_COLS)
        all_keys = ", ".join(f"_k{i}" for i in range(n_keys))
        # hash of concatenated key string — single-arg hash(), portable
        hash_expr = " || chr(1) || ".join(
            f"COALESCE(_k{i}, '')" for i in range(n_keys)
        )
        str_key_sql = "\n".join(
            f"COALESCE(CAST({c} AS VARCHAR), '') AS _k{i},"
            for i, c in enumerate(_STR_KEY_COLS)
        )
        list_key_sql = "\n".join(
            f"CASE WHEN {c} IS NOT NULL AND len({c}) > 0"
            f" THEN CAST({c}[1] AS VARCHAR) ELSE NULL END AS _k{i + len(_STR_KEY_COLS)}{',' if i < len(_LIST_KEY_COLS) - 1 else ''}"
            for i, c in enumerate(_LIST_KEY_COLS)
        )
        exclude = ", ".join(f"_k{i}" for i in range(n_keys)) + ", _rn"

        print(
            f"  [{_ts()}] DuckDB dedup: {n_parts} hash-partitions × window ROW_NUMBER+COUNT "
            f"(mem={memory_gb}GB, threads={n_threads}) ...",
            flush=True,
        )
        for p in range(n_parts):
            part_path = duck_dir / f"dedup_{p}.parquet"
            if part_path.exists():
                rows_p = int(pl.scan_parquet(str(part_path)).select(pl.len().alias("n")).collect()["n"][0])
                print(f"  [{_ts()}]   Partition {p+1}/{n_parts}: RESUME ({rows_p:,} rows)", flush=True)
                continue

            sql_p = f"""
            COPY (
                WITH keyed AS (
                    SELECT *,
                           {str_key_sql}
                           {list_key_sql}
                    FROM read_parquet('{src_path}')
                ),
                ranked AS (
                    SELECT *,
                           ROW_NUMBER() OVER (
                               PARTITION BY {all_keys}
                               ORDER BY safetyreportid, entry
                           ) AS _rn,
                           COUNT(*) OVER (PARTITION BY {all_keys}) AS source_record_count
                    FROM keyed
                    WHERE hash({hash_expr}) % {n_parts} = {p}
                )
                SELECT * EXCLUDE ({exclude})
                FROM ranked
                WHERE _rn = 1
            ) TO '{part_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """
            t1 = time.time()
            con = duckdb.connect(":memory:")
            try:
                con.execute(f"SET memory_limit='{memory_gb}GB'")
                con.execute(f"SET threads={n_threads}")
                con.execute(f"SET temp_directory='{duck_dir}'")
                con.execute(sql_p)
            finally:
                con.close()

            rows_p = int(pl.scan_parquet(str(part_path)).select(pl.len().alias("n")).collect()["n"][0])
            print(f"  [{_ts()}]   Partition {p+1}/{n_parts}: {rows_p:,} rows in {time.time()-t1:.0f}s", flush=True)

        # Concatenate all partitions + assign global mapping_record_id
        print(f"  [{_ts()}] Concatenating {n_parts} dedup partitions ...", flush=True)
        part_files = [str(duck_dir / f"dedup_{p}.parquet") for p in range(n_parts)]
        (
            pl.scan_parquet(part_files)
            .with_row_index("mapping_record_id", offset=1)
            .sink_parquet(str(out_path), compression="zstd")
        )
        for pf in part_files:
            Path(pf).unlink(missing_ok=True)
        try:
            duck_dir.rmdir()
        except OSError:
            pass

    else:
        # ── Polars lazy fallback: two streaming passes (no eager read_parquet) ──
        schema = pl.scan_parquet(str(src_path)).collect_schema()
        col_names = set(schema.names())

        def _lazy_key(col: str, is_list: bool) -> pl.Expr:
            alias = f"_k_{col}"
            if col not in col_names:
                return pl.lit(None, dtype=pl.Utf8).alias(alias)
            if is_list:
                return (
                    pl.when(pl.col(col).is_not_null() & (pl.col(col).list.len() > 0))
                    .then(pl.col(col).list.first().cast(pl.Utf8))
                    .otherwise(pl.lit(None, dtype=pl.Utf8))
                    .alias(alias)
                )
            return pl.col(col).cast(pl.Utf8, strict=False).fill_null("").alias(alias)

        key_exprs = (
            [_lazy_key(c, False) for c in _STR_KEY_COLS]
            + [_lazy_key(c, True)  for c in _LIST_KEY_COLS]
        )
        dedup_cols = [f"_k_{c}" for c in _STR_KEY_COLS + _LIST_KEY_COLS
                      if c in col_names or True]

        base_lf = pl.scan_parquet(str(src_path)).with_columns(key_exprs)

        # Pass 1: counts per unique drug profile
        count_lf = (
            base_lf.group_by(dedup_cols)
            .agg(pl.len().alias("source_record_count"))
        )
        # Pass 2: keep first row per unique drug profile, join counts
        (
            base_lf
            .unique(subset=dedup_cols, keep="first")
            .join(count_lf, on=dedup_cols, how="left")
            .drop(dedup_cols)
            .with_row_index("mapping_record_id", offset=1)
            .sink_parquet(str(out_path), compression="zstd")
        )

    rows = int(pl.scan_parquet(str(out_path)).select(pl.len().alias("n")).collect()["n"][0])
    print(f"  [{_ts()}] ✅ drug_mapping_input_unique: {rows:,} rows in {time.time() - t0:.0f}s{_log_mem()}", flush=True)
    logger.info("[S02-stream] drug_mapping_input_unique rows={}", rows)
    return {"rows": rows, "path": str(out_path)}


def _build_reaction_table(root: Path, out_dir: Path) -> Dict[str, object]:
    reaction_dir = root / "patient_reaction"
    reaction_map = {
        "reactionmeddrapt": "reaction_meddrapt",
        "reactionoutcome": "reaction_outcome",
    }
    reaction_lf = (
        _scan_csv(
            reaction_dir,
            ["safetyreportid", *reaction_map.keys()],
            {"safetyreportid": pl.Utf8},
        )
        .select(
            pl.col("safetyreportid").cast(pl.Utf8),
            *[
                pl.col(src).cast(pl.Utf8, strict=False).alias(dst)
                for src, dst in reaction_map.items()
            ],
        )
        .filter(pl.col("safetyreportid").is_not_null())
        .filter(pl.col("reaction_meddrapt").is_not_null())
        .unique()
    )
    reaction_path = out_dir / "reactions.parquet"
    reaction_rows = _write(reaction_lf, reaction_path)
    logger.info("[S02-stream] reactions rows={}", reaction_rows)
    return {"rows": reaction_rows, "path": str(reaction_path)}


def _run_drug_validation(out_dir: Path) -> Dict[str, object]:
    """Validate S02 drug tables and export a JSON quality report.

    Checks performed per table:
      1. Row count
      2. (safetyreportid, entry) uniqueness — only for tables where this is the PK
      3. Null coverage for key identifier columns

    Exports drug_mapping_input_quality_report.json beside the parquet files.
    """
    import json

    # ── Config ────────────────────────────────────────────────────────────────
    KEY_COLS   = ["safetyreportid", "entry"]
    COV_COLS   = [
        "medicinal_product",
        "active_substance_name",
        "openfda_rxcui",
        "openfda_generic_name",
    ]
    # Only these tables are expected to have unique (safetyreportid, entry)
    UNIQUE_KEY_TABLES = {"drugcharacteristics_extended", "drug_mapping_input"}
    # Tables to validate
    VALIDATE_TABLES = [
        "drugcharacteristics_extended",
        "drug_mapping_input",
    ]

    sep = "─" * 60
    print(f"\n{sep}", flush=True)
    results: Dict[str, object] = {}
    any_fail = False

    for name in VALIDATE_TABLES:
        path = out_dir / f"{name}.parquet"
        print(f"\n  [{_ts()}] Validating {name} ...", flush=True)

        if not path.exists():
            results[name] = {"status": "FAIL", "error": "file_not_found"}
            print(f"  ❌  FAIL — file not found: {path}", flush=True)
            any_fail = True
            logger.warning("[S02] Validation skipped for {} — file not found", name)
            continue

        try:
            schema = pl.scan_parquet(str(path)).collect_schema()
            avail_cov = [c for c in COV_COLS if c in schema.names()]

            # ── Pass 1: row count + null coverage (single scan) ────────────
            agg_exprs: list = [pl.len().alias("_total")]
            for col in avail_cov:
                dtype = schema[col]
                is_list = hasattr(dtype, "inner")  # List types have .inner
                if is_list:
                    agg_exprs.append(
                        (pl.col(col).is_not_null() & (pl.col(col).list.len() > 0))
                        .sum().alias(f"_nn_{col}")
                    )
                else:
                    agg_exprs.append(
                        (
                            pl.col(col).is_not_null()
                            & (pl.col(col).cast(pl.Utf8, strict=False).str.len_chars() > 0)
                        ).sum().alias(f"_nn_{col}")
                    )

            stats = pl.scan_parquet(str(path)).select(agg_exprs).collect()
            total_rows = int(stats["_total"][0])

            if total_rows == 0:
                results[name] = {"status": "WARN", "total_rows": 0, "unique_keys": 0,
                                 "duplicate_rows": 0, "coverage": {}}
                print(f"  ⚠  {name}: 0 rows — empty table", flush=True)
                continue

            # ── Pass 2: uniqueness check (only for PK tables) ──────────────
            unique_keys  = total_rows
            dup_groups   = 0
            duplicate_rows = 0

            if name in UNIQUE_KEY_TABLES:
                avail_keys = [c for c in KEY_COLS if c in schema.names()]
                if len(avail_keys) == len(KEY_COLS):
                    key_stats = (
                        pl.scan_parquet(str(path))
                        .group_by(avail_keys)
                        .agg(pl.len().alias("_n"))
                        .select([
                            pl.len().alias("unique_key_count"),
                            (pl.col("_n") > 1).sum().alias("dup_group_count"),
                            (pl.col("_n").sum() - pl.len()).alias("extra_rows"),
                        ])
                        .collect()
                    )
                    unique_keys    = int(key_stats["unique_key_count"][0])
                    dup_groups     = int(key_stats["dup_group_count"][0])
                    duplicate_rows = int(key_stats["extra_rows"][0])

            # ── Build coverage dict ────────────────────────────────────────
            coverage: Dict[str, object] = {}
            for col in avail_cov:
                nn  = int(stats[f"_nn_{col}"][0])
                pct = round(nn / total_rows * 100, 2)
                coverage[col] = {"non_null": nn, "total": total_rows, "pct": pct}

            # ── Determine pass/fail ────────────────────────────────────────
            passed = duplicate_rows == 0
            status = "PASS" if passed else "FAIL"
            icon   = "✅" if passed else "❌"
            if not passed:
                any_fail = True

            result: Dict[str, object] = {
                "status":         status,
                "total_rows":     total_rows,
                "unique_keys":    unique_keys,
                "duplicate_rows": duplicate_rows,
                "dup_groups":     dup_groups,
                "coverage":       coverage,
            }
            results[name] = result

            # ── Print summary ──────────────────────────────────────────────
            print(
                f"  {icon} {status}  {name}\n"
                f"       total_rows    : {total_rows:>15,}\n"
                f"       unique_keys   : {unique_keys:>15,}\n"
                f"       duplicate_rows: {duplicate_rows:>15,}  "
                f"{'← DUPLICATES FOUND' if duplicate_rows > 0 else ''}",
                flush=True,
            )
            for col, cov in coverage.items():
                flag = "✓" if cov["pct"] >= 50 else "⚠ LOW"  # type: ignore[index]
                print(
                    f"       {flag:<6} {col}: "
                    f"{cov['non_null']:,} / {cov['total']:,}  ({cov['pct']:.1f}%)",  # type: ignore[index]
                    flush=True,
                )

            if duplicate_rows > 0:
                logger.warning(
                    "[S02] {} has {} duplicate (safetyreportid, entry) rows across {} groups",
                    name, duplicate_rows, dup_groups,
                )

        except Exception as exc:
            results[name] = {"status": "ERROR", "error": str(exc)}
            any_fail = True
            logger.warning("[S02] Validation failed for {}: {}", name, exc)
            print(f"  ❌  ERROR validating {name}: {exc}", flush=True)

    # ── Export JSON quality report for drug_mapping_input ─────────────────────
    dmi = results.get("drug_mapping_input", {})
    if isinstance(dmi, dict) and "total_rows" in dmi:
        report: Dict[str, object] = {
            "table":          "drug_mapping_input",
            "total_rows":     dmi["total_rows"],
            "unique_keys":    dmi["unique_keys"],
            "duplicate_rows": dmi["duplicate_rows"],
            "coverage":       dmi["coverage"],
            "pass":           dmi["status"] == "PASS",
        }
        report_path = out_dir / "drug_mapping_input_quality_report.json"
        with open(str(report_path), "w", encoding="utf-8") as fh:
            json.dump(report, fh, indent=2, ensure_ascii=False)
        print(f"\n  📄 Quality report → {report_path}", flush=True)

    # ── Overall summary ───────────────────────────────────────────────────────
    overall_status = "✅ ALL CHECKS PASSED" if not any_fail else "❌ SOME CHECKS FAILED"
    print(f"\n  {overall_status}", flush=True)
    print(sep, flush=True)

    results["_overall_pass"] = not any_fail
    return results


def _skip_if_exists(out_path: Path, label: str) -> "Dict[str, object] | None":
    """Return a manifest entry (with skipped=True) if out_path already exists.

    Used to make S02 resumable: if a table was successfully written in a previous
    (possibly failed) run, skip it instead of rebuilding from scratch.
    Returns None when the file does NOT exist (proceed with normal build).
    """
    if not out_path.exists():
        return None
    try:
        rows = int(
            pl.scan_parquet(str(out_path))
            .select(pl.len().alias("n"))
            .collect()["n"][0]
        )
    except Exception:
        return None  # unreadable → rebuild
    print(f"  ⏭  {label} already exists ({rows:,} rows) — skipped", flush=True)
    return {"rows": rows, "path": str(out_path), "skipped": True}


def run(ctx: PipelineContext) -> None:
    root = _openfda_root(ctx)
    print(f"[S02] Input  : {root}", flush=True)

    stage_name = ctx.config.metadata.get("s02_stream_stage", "s02_entity_format")
    out_dir = stage_output_path(ctx, stage_name)
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"[S02] Output : {out_dir}", flush=True)
    print("=" * 60, flush=True)

    t_total = time.time()
    manifest: Dict[str, Dict[str, object]] = {}

    # ── Original outputs (backward-compatible) ────────────────────────────────
    print("\n[S02] (1/7) Report tables  (report, report_serious, reporter)", flush=True)
    cached = [_skip_if_exists(out_dir / "report.parquet", "report.parquet"),
              _skip_if_exists(out_dir / "report_serious.parquet", "report_serious.parquet"),
              _skip_if_exists(out_dir / "reporter.parquet", "reporter.parquet")]
    if all(cached):
        manifest.update({"report": cached[0], "report_serious": cached[1], "reporter": cached[2]})
    else:
        manifest.update(_build_report_tables(root, out_dir))

    print("\n[S02] (2/7) Patient table", flush=True)
    manifest["patient"] = (
        _skip_if_exists(out_dir / "patient.parquet", "patient.parquet")
        or _build_patient_table(root, out_dir)
    )

    print("\n[S02] (3/7) Drug characteristics table  [original, backward-compat]", flush=True)
    manifest["drugcharacteristics"] = (
        _skip_if_exists(out_dir / "drugcharacteristics.parquet", "drugcharacteristics.parquet")
        or _build_drug_table(root, out_dir)
    )

    print("\n[S02] (4/7) Reaction table", flush=True)
    manifest["reactions"] = (
        _skip_if_exists(out_dir / "reactions.parquet", "reactions.parquet")
        or _build_reaction_table(root, out_dir)
    )

    # ── New drug-level outputs for advanced mapping ───────────────────────────
    print("\n[S02] (5/7) Drug extended table  [new: all drug fields, keyed by (safetyreportid, entry)]", flush=True)
    manifest["drugcharacteristics_extended"] = (
        _skip_if_exists(out_dir / "drugcharacteristics_extended.parquet", "drugcharacteristics_extended.parquet")
        or _build_drug_extended_table(root, out_dir)
    )

    print("\n[S02] (6/7) Drug openfda wide table  [new: pivoted openfda key-value → List columns]", flush=True)
    manifest["drug_openfda_wide"] = (
        _skip_if_exists(out_dir / "drug_openfda_wide.parquet", "drug_openfda_wide.parquet")
        or _build_drug_openfda_wide(root, out_dir, ctx)
    )

    print("\n[S02] (7a/7) Drug mapping input  [new: extended LEFT JOIN wide]", flush=True)
    manifest["drug_mapping_input"] = (
        _skip_if_exists(out_dir / "drug_mapping_input.parquet", "drug_mapping_input.parquet")
        or _build_drug_mapping_input(out_dir)
    )

    print("\n[S02] (7b/7) Drug mapping input (unique)  [new: deduplicated mapping candidates]", flush=True)
    manifest["drug_mapping_input_unique"] = (
        _skip_if_exists(out_dir / "drug_mapping_input_unique.parquet", "drug_mapping_input_unique.parquet")
        or _build_drug_mapping_input_unique(out_dir)
    )

    # ── Validation: duplicate check + non-null coverage ─────────────────────
    print("\n[S02] Validation (duplicate check, non-null coverage)", flush=True)
    validation = _run_drug_validation(out_dir)
    manifest["_validation"] = validation

    elapsed = time.time() - t_total
    total_rows = sum(
        v["rows"] for v in manifest.values() if isinstance(v, dict) and "rows" in v
    )
    print(
        f"\n[S02] ✅ All tables done — {total_rows:,} total rows in {elapsed:.0f}s ({elapsed/60:.1f} min)",
        flush=True,
    )
    print(f"[S02] Output directory: {out_dir}", flush=True)

    write_manifest(
        ctx,
        stage_name,
        {"stage": stage_name, "input_root": str(root), "tables": manifest},
    )
    logger.success("[S02-stream] ER tables written to {}", out_dir)


def run_validation_only(out_dir: "Path | str | None" = None) -> Dict[str, object]:
    """Run S02 validation standalone without rebuilding any tables.

    Usage (from project root):
        python -m src.stages.s02_entity_format_stream --validate-only
        python -m src.stages.s02_entity_format_stream --validate-only --out-dir /path/to/staging/s02_entity_format

    Returns the validation results dict (same as _run_drug_validation).
    """
    if out_dir is None:
        # Try to resolve from config
        try:
            from src.settings import load_settings
            from src.utils.io import init_run_context, stage_output_path
            cfg = load_settings()
            ctx = init_run_context(cfg)
            stage_name = cfg.metadata.get("s02_stream_stage", "s02_entity_format")
            out_dir = stage_output_path(ctx, stage_name)
        except Exception as exc:
            raise RuntimeError(
                "Cannot determine S02 output directory. "
                "Pass --out-dir explicitly or set up config.local.yaml."
            ) from exc

    out_dir = Path(out_dir)
    print(f"\n[S02-validate] Output directory: {out_dir}", flush=True)
    print(f"[S02-validate] Running validation only (no table rebuild) ...", flush=True)
    results = _run_drug_validation(out_dir)
    return results


if __name__ == "__main__":
    import argparse
    from src.settings import load_settings
    from src.utils.io import init_run_context

    parser = argparse.ArgumentParser(description="Run the streaming S02 entity format stage.")
    parser.add_argument(
        "--config",
        help="Path to pipeline config YAML (defaults to env/metadata).",
    )
    parser.add_argument(
        "--run-id",
        help="Override run identifier for logging/output.",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Skip all table builds — only run validation + export quality report.",
    )
    parser.add_argument(
        "--out-dir",
        help="S02 output directory (required when --validate-only and no config).",
    )
    args = parser.parse_args()

    if args.validate_only:
        run_validation_only(out_dir=args.out_dir)
    else:
        config = load_settings(args.config) if args.config else load_settings()
        ctx = init_run_context(config, run_id=args.run_id)
        run(ctx)
