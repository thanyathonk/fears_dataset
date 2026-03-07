from __future__ import annotations

"""Alternative Stage S02 implementation using Polars streaming transforms.

This module reads the OpenFDA flat CSV outputs emitted by S01 and produces
normalized ER tables as Parquet files under the S02 staging directory.
It replicates the key tables consumed by later pipeline stages while avoiding
large in-memory pandas concatenations.
"""

import time
from pathlib import Path
from typing import Dict, Iterable, Mapping, Sequence

if __package__ is None or __package__ == "":
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import os
import subprocess

import polars as pl
from loguru import logger

try:
    import pandas as pd
    _HAS_PANDAS = True
except ImportError:
    _HAS_PANDAS = False

from src.utils.io import PipelineContext, stage_output_path, write_manifest

__all__ = ["run"]

_NULLS = ["", "NA", "NaN", "null", "NULL"]
_PATTERNS = ("*.csv", "*.csv.gz", "*.csv.gzip")


def _use_high_memory_openfda(ctx: PipelineContext) -> bool:
    """True when S02 should use in-memory path for openfda (no disk shards).

    Set via env S02_OPENFDA_HIGH_MEMORY=1 or config stages.s02_openfda_high_memory: true.
    Requires ~80–120GB RAM. Simpler and faster on high-RAM machines.
    """
    if os.environ.get("S02_OPENFDA_HIGH_MEMORY", "").strip().lower() in ("1", "true", "yes"):
        return True
    return bool(getattr(getattr(ctx.config, "stages", None), "s02_openfda_high_memory", False))


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
    """Return False if file is truncated/corrupt gzip (avoids segfault in decompression)."""
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
) -> "pd.DataFrame | None":
    """Read CSV with pandas, aggregate by (safetyreportid, entry, key). Returns DataFrame or None."""
    if not _is_gzip_ok(file_path) or not _HAS_PANDAS:
        return None
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
        if pdf.empty or not needed.issubset(pdf.columns):
            return None
        pdf = pdf[list(needed)].dropna(subset=["safetyreportid", "key", "value"])
        pdf = pdf[pdf["key"].isin(known_keys)]
        if pdf.empty:
            return None
        pdf["entry"] = pd.to_numeric(pdf["entry"], errors="coerce").fillna(0).astype("int64")
        agg = (
            pdf.groupby(["safetyreportid", "entry", "key"], dropna=False)["value"]
            .apply(lambda s: list(s.dropna().unique()))
            .reset_index()
        )
        agg.columns = ["safetyreportid", "entry", "key", "values"]
        return agg
    except Exception:
        return None


def _process_openfda_file_to_shard(
    file_path: Path,
    shard_tmp: Path,
    shard_out: Path,
    known_keys: list,
) -> bool:
    """Read CSV, aggregate, and write shard. Uses pandas+pyarrow to avoid Polars segfault."""
    agg = _read_and_aggregate_openfda_file(Path(file_path), known_keys)
    if agg is None:
        return False
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq

        table = pa.Table.from_pandas(agg, preserve_index=False)
        pq.write_table(table, str(shard_tmp), compression="zstd")
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


def _scan_csv(
    folder: Path,
    columns: Iterable[str],
    dtypes: Mapping[str, pl.PolarsDataType] | None = None,
) -> pl.LazyFrame:
    files = _enumerate_csvs(folder)
    required = list(columns)
    dtype_map = dict(dtypes or {})

    lazy_frames: list[pl.LazyFrame] = []
    for file_path in files:
        sample = pl.read_csv(
            file_path,
            has_header=True,
            n_rows=0,
            null_values=_NULLS,
        )
        available = set(sample.columns)
        schema_overrides = {col: dtype_map[col] for col in available if col in dtype_map}

        common_kwargs = dict(
            has_header=True,
            infer_schema_length=0,
            null_values=_NULLS,
            ignore_errors=True,
        )
        try:
            lf = pl.scan_csv(file_path, schema_overrides=schema_overrides, **common_kwargs)
        except TypeError:
            lf = pl.scan_csv(file_path, dtypes=schema_overrides, **common_kwargs)

        keep = [col for col in required if col in available]
        if keep:
            lf = lf.select([pl.col(col) for col in keep])
        else:
            lf = lf.select([])

        missing = [col for col in required if col not in available]
        if missing:
            lf = lf.with_columns(
                [
                    pl.lit(None, dtype=dtype_map.get(col, pl.Utf8)).alias(col)
                    for col in missing
                ]
            )

        lf = lf.select([pl.col(col) for col in required])
        lazy_frames.append(lf)

    if not lazy_frames:
        raise FileNotFoundError(f"No CSV files found in {folder}")

    return pl.concat(lazy_frames, how="diagonal_relaxed")


def _write(lf: pl.LazyFrame, path: Path) -> int:
    t0 = time.time()
    print(f"  → Writing {path.name} ...", flush=True)
    path.parent.mkdir(parents=True, exist_ok=True)
    lf.sink_parquet(path, compression="zstd")
    rows = int(
        pl.scan_parquet(path)
        .select(pl.len().alias("rows"))
        .collect(streaming=True)["rows"][0]
    )
    print(f"  ✅ {path.name}: {rows:,} rows  ({time.time() - t0:.1f}s)", flush=True)
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
        "[S02-stream] report=%d report_serious=%d reporter=%d",
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
    logger.info("[S02-stream] patient rows=%d", patient_rows)
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
    logger.info("[S02-stream] drugcharacteristics rows=%d", drug_rows)
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
    "drugdosagetext":                 "drug_dosage_text",
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
    # NOTE: activesubstance / activesubstance_name are handled separately below
    # via _extract_active_substance_name_expr() — not in this rename map.
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
    """Return a vectorized Polars expression that parses active_substance_name
    from a column containing the raw S01 'activesubstance' string.

    S01 stores this field as the Python repr of a dict (or list of dicts), e.g.:
        {'activesubstancename': 'DUPILUMAB'}
        {"activesubstancename": "DUPILUMAB"}
        {'activesubstancename' : 'DUPILUMAB'}   (spaces around colon)
        [{'activesubstancename': 'A'}, ...]     (list form — first match captured)

    Regex strategy (single vectorized str.extract call):
        ['"]activesubstancename['"]\\s*:\\s*['"]([^'"]+)['"]
        key with either quote type              value captured (no quotes inside)

    Post-processing (all vectorized):
        1. strip_chars()           trim leading/trailing whitespace
        2. replace_all(r'\\s{2,}') collapse multiple internal spaces
        3. null-out empty strings  catches parse failures cleanly

    Casing: preserves original (FAERS substance names are conventionally UPPERCASE).
    Returns null when the source is null, empty, or doesn't match any pattern.
    """
    raw = pl.col(raw_col).cast(pl.Utf8, strict=False)

    # Unified regex: handles both ' and " quotes around key and value,
    # with any amount of whitespace around the colon separator.
    pattern = r"""['"]activesubstancename['"]\s*:\s*['"]([^'"]+)['"]"""

    parsed = (
        raw
        .str.extract(pattern, 1)
        .str.strip_chars()
        .str.replace_all(r"\s{2,}", " ")
    )

    # Return null for empty-string results (parse failure or genuinely empty value)
    return (
        pl.when(parsed.is_not_null() & (parsed.str.len_chars() > 0))
        .then(parsed)
        .otherwise(pl.lit(None, dtype=pl.Utf8))
    )


def _build_drug_extended_table(root: Path, out_dir: Path) -> Dict[str, object]:
    """Build drugcharacteristics_extended.parquet.

    Unit: 1 row = 1 (safetyreportid, entry) drug record — NOT de-duplicated.
    `entry` is the 0-based index of the drug within its safety report (from S01).

    Output columns include:
      - safetyreportid, entry              — primary key
      - [all fields from _DRUG_EXTENDED_SRC_TO_DST] — dosage, authorization, etc.
      - activesubstance_raw                — raw string from S01 CSV for audit/debug
      - active_substance_name              — parsed substance name for drug mapping

    activesubstance parsing strategy (backward-compatible, no S01 re-run needed):
      Priority 1: 'activesubstance_name' column (only present after new S01 runs)
      Priority 2: regex parse of 'activesubstance' legacy column
                  (handles both single-/double-quoted Python dict repr)
      Fallback:   null (parse failure logged in manifest stats)
    """
    drug_dir = root / "patient_drug"

    src_cols = list(_DRUG_EXTENDED_SRC_TO_DST.keys())
    # Request both the legacy 'activesubstance' column AND the new 'activesubstance_name'
    # column from S01. _scan_csv fills missing columns with null — safe for all file ages.
    all_required = ["safetyreportid", "entry", "activesubstance", "activesubstance_name"] + src_cols

    lf = _scan_csv(
        drug_dir,
        all_required,
        {"safetyreportid": pl.Utf8, "entry": pl.Int64},
    )

    # ── Core key casts ────────────────────────────────────────────────────────
    lf = lf.with_columns(
        pl.col("safetyreportid").cast(pl.Utf8, strict=False),
        pl.col("entry").cast(pl.Int64, strict=False),
        # Preserve the raw activesubstance string for audit/debug.
        # Stored as-is (Python dict repr) from S01.
        pl.col("activesubstance").cast(pl.Utf8, strict=False).alias("activesubstance_raw"),
    )

    # ── Parse active_substance_name ───────────────────────────────────────────
    # Coalesce:
    #   1. New S01 format: clean 'activesubstance_name' column
    #   2. Legacy format: regex-parsed from 'activesubstance_raw'
    lf = lf.with_columns(
        pl.coalesce([
            pl.col("activesubstance_name").cast(pl.Utf8, strict=False),
            _extract_active_substance_name_expr("activesubstance_raw"),
        ]).alias("active_substance_name")
    )

    # Drop intermediate columns no longer needed
    lf = lf.drop("activesubstance", "activesubstance_name")

    # ── Final column selection ────────────────────────────────────────────────
    # safetyreportid + entry + all renamed drug fields + raw + parsed substance
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
    )

    out_path = out_dir / "drugcharacteristics_extended.parquet"
    rows = _write(lf, out_path)

    # ── Coverage stats ────────────────────────────────────────────────────────
    stats = (
        pl.scan_parquet(str(out_path))
        .select(
            pl.col("activesubstance_raw").is_not_null().sum().alias("raw_non_null"),
            pl.col("active_substance_name").is_not_null().sum().alias("parsed_non_null"),
            pl.len().alias("total"),
        )
        .collect()
    )
    raw_nn    = int(stats["raw_non_null"][0])
    parsed_nn = int(stats["parsed_non_null"][0])
    total     = int(stats["total"][0])
    cov_raw    = raw_nn    / total * 100 if total else 0
    cov_parsed = parsed_nn / total * 100 if total else 0
    print(
        f"  activesubstance_raw    : {raw_nn:>12,} / {total:,} rows  ({cov_raw:.1f}%)",
        flush=True,
    )
    print(
        f"  active_substance_name  : {parsed_nn:>12,} / {total:,} rows  ({cov_parsed:.1f}%)  "
        f"[parse success rate: {parsed_nn / raw_nn * 100:.1f}% of non-null raws]"
        if raw_nn else f"  active_substance_name  : {parsed_nn:>12,} / {total:,} rows",
        flush=True,
    )
    logger.info(
        "[S02-stream] drugcharacteristics_extended rows=%d  "
        "activesubstance_raw=%d (%.1f%%)  active_substance_name=%d (%.1f%%)",
        rows, raw_nn, cov_raw, parsed_nn, cov_parsed,
    )
    return {
        "rows": rows,
        "path": str(out_path),
        "activesubstance_raw_count":    raw_nn,
        "active_substance_name_count":  parsed_nn,
        "active_substance_name_coverage_pct": round(cov_parsed, 2),
    }


def _openfda_collect_per_file(
    openfda_dir: Path,
    known_keys: list,
    tmp_dir: Path,
) -> pl.LazyFrame:
    """Disk-based 2-pass collection for patient_drug_openfda CSVs.

    Pass 1 (Map):    Read each CSV eagerly → filter → group_by → write to a temp
                     parquet shard in tmp_dir.  Files already present are skipped,
                     making this step fully resumable.

    Pass 2 (Reduce): Scan all shard parquets lazily (memory-efficient) and return
                     a LazyFrame for the caller to do the final group_by + pivot.

    Corrupt or unreadable files are skipped with a warning.

    Returns a LazyFrame with columns: safetyreportid, entry, key, values (List[Utf8]).
    """
    tmp_dir.mkdir(parents=True, exist_ok=True)

    # Clean up any leftover .tmp files from a previously killed run
    for stale in tmp_dir.glob("*.parquet.tmp"):
        stale.unlink(missing_ok=True)

    files = _enumerate_csvs(openfda_dir)
    skipped: list = []
    written = 0
    already = 0

    for file_path in files:
        shard_out = tmp_dir / (Path(file_path).stem + ".parquet")
        shard_tmp = tmp_dir / (Path(file_path).stem + ".parquet.tmp")
        if shard_out.exists():
            already += 1
            continue
        ok = _process_openfda_file_to_shard(
            Path(file_path), shard_tmp, shard_out, known_keys
        )
        if ok:
            written += 1
        else:
            skipped.append(file_path)

    if skipped:
        print(f"  ⚠  Skipped {len(skipped)} file(s) due to read errors.", flush=True)

    print(
        f"  → Per-file shards: {written} written, {already} reused, {len(skipped)} skipped",
        flush=True,
    )

    shard_files = sorted(tmp_dir.glob("*.parquet"))
    if not shard_files:
        return pl.LazyFrame({
            "safetyreportid": pl.Series([], dtype=pl.Utf8),
            "entry":          pl.Series([], dtype=pl.Int64),
            "key":            pl.Series([], dtype=pl.Utf8),
            "values":         pl.Series([], dtype=pl.List(pl.Utf8)),
        })

    # Lazy scan across all shards — no data loaded into RAM yet
    return (
        pl.scan_parquet([str(f) for f in shard_files])
        .group_by(["safetyreportid", "entry", "key"])
        .agg(pl.col("values").flatten().drop_nulls().alias("values"))
    )


def _build_openfda_wide_high_memory(
    openfda_dir: Path,
    out_path: Path,
    known_keys: list,
) -> Dict[str, object]:
    """In-memory path: read all files with pandas, concat, groupby, pivot, write.

    Requires ~80–120GB RAM. No disk shards. Simpler and faster on high-RAM machines.
    """
    if not _HAS_PANDAS:
        raise RuntimeError("High-memory requires pandas. Install with: pip install pandas")

    files = _enumerate_csvs(openfda_dir)
    aggs: list = []
    skipped = 0

    print("  → High-memory: reading and aggregating all files in memory...", flush=True)
    for i, file_path in enumerate(files):
        agg = _read_and_aggregate_openfda_file(Path(file_path), known_keys)
        if agg is not None:
            aggs.append(agg)
        else:
            skipped += 1
        if (i + 1) % 200 == 0:
            print(f"    processed {i + 1}/{len(files)} files...", flush=True)

    if skipped:
        print(f"  ⚠  Skipped {skipped} file(s) due to read errors.", flush=True)

    if not aggs:
        pl.DataFrame({
            "safetyreportid": pl.Series([], dtype=pl.Utf8),
            "entry":          pl.Series([], dtype=pl.Int64),
        }).write_parquet(str(out_path), compression="zstd")
        return {"rows": 0, "path": str(out_path), "keys_found": [], "skipped_files": skipped}

    print("  → Concatenating and merging...", flush=True)
    combined = pd.concat(aggs, ignore_index=True)

    # Merge duplicate (safetyreportid, entry, key) across files
    def _merge_values(series):
        out = []
        for x in series:
            if isinstance(x, list):
                out.extend(v for v in x if pd.notna(v) and v != "")
            elif pd.notna(x) and x != "":
                out.append(x)
        return list(dict.fromkeys(out))  # preserve order, dedupe

    combined = (
        combined.groupby(["safetyreportid", "entry", "key"], dropna=False)["values"]
        .apply(_merge_values)
        .reset_index()
    )

    print("  → Pivoting to wide format...", flush=True)
    wide = combined.pivot_table(
        index=["safetyreportid", "entry"],
        columns="key",
        values="values",
        aggfunc="first",
    )

    # Rename columns to openfda_{key}
    wide.columns = [f"openfda_{k}" for k in wide.columns]
    wide = wide.reset_index()

    print("  → Writing parquet...", flush=True)
    pl.from_pandas(wide).write_parquet(str(out_path), compression="zstd")
    rows = len(wide)
    keys_found = sorted(combined["key"].unique().tolist())
    logger.info("[S02-stream] drug_openfda_wide (high-memory) rows=%d  keys=%s", rows, keys_found)
    return {
        "rows": rows,
        "path": str(out_path),
        "keys_found": keys_found,
        "skipped_files": skipped,
    }


def _build_drug_openfda_wide(root: Path, out_dir: Path, ctx: PipelineContext) -> Dict[str, object]:
    """Build drug_openfda_wide.parquet — pivoted openfda key-value table.

    Unit: 1 row = 1 (safetyreportid, entry).
    Each openfda key becomes a List[Utf8] column (e.g. openfda_rxcui, openfda_generic_name).

    Two modes (auto-selected by env/config):
      - High-memory: S02_OPENFDA_HIGH_MEMORY=1 or stages.s02_openfda_high_memory: true
        Requires ~80–120GB RAM. In-memory, no disk shards. Simpler and faster.
      - Low-memory (default): Disk-based 2-pass, resumable. Works with ~16GB RAM.
    """
    openfda_dir = root / "patient_drug_openfda"
    out_path = out_dir / "drug_openfda_wide.parquet"

    _EMPTY_PLACEHOLDER = pl.DataFrame({
        "safetyreportid": pl.Series([], dtype=pl.Utf8),
        "entry":          pl.Series([], dtype=pl.Int64),
    })

    if not openfda_dir.exists() or not list(openfda_dir.glob("*.csv*")):
        logger.warning("[S02-stream] No patient_drug_openfda CSV files found — skipping wide table")
        _EMPTY_PLACEHOLDER.write_parquet(str(out_path), compression="zstd")
        return {"rows": 0, "path": str(out_path), "keys_found": [], "skipped_files": 0}

    if _use_high_memory_openfda(ctx):
        print("  → Using high-memory path (S02_OPENFDA_HIGH_MEMORY or config)", flush=True)
        return _build_openfda_wide_high_memory(openfda_dir, out_path, OPENFDA_KNOWN_KEYS)

    # ── Disk-based 2-pass (low-memory) ────────────────────────────────────────
    tmp_dir = out_dir / "_tmp_openfda_shards"
    print("  → Pass 1: per-file aggregation to disk shards...", flush=True)
    agg_lf = _openfda_collect_per_file(openfda_dir, OPENFDA_KNOWN_KEYS, tmp_dir)

    print("  → Pass 2: collecting aggregated shards and pivoting...", flush=True)
    agg_df = agg_lf.collect()

    if agg_df.is_empty():
        logger.warning("[S02-stream] openfda table empty after filtering — writing placeholder")
        _EMPTY_PLACEHOLDER.write_parquet(str(out_path), compression="zstd")
        return {"rows": 0, "path": str(out_path), "keys_found": [], "skipped_files": 0}

    keys_found = sorted(agg_df["key"].unique().to_list())
    print(f"  → openfda keys found: {keys_found}", flush=True)

    wide_df = agg_df.pivot(
        index=["safetyreportid", "entry"],
        on="key",
        values="values",
        aggregate_function="first",
    )
    rename_map = {k: f"openfda_{k}" for k in keys_found if k in wide_df.columns}
    wide_df = wide_df.rename(rename_map)

    wide_df.write_parquet(str(out_path), compression="zstd")
    rows = wide_df.height
    logger.info("[S02-stream] drug_openfda_wide rows=%d  keys=%s", rows, keys_found)
    return {
        "rows": rows,
        "path": str(out_path),
        "keys_found": keys_found,
        "skipped_files": 0,
    }


def _build_drug_mapping_input(out_dir: Path) -> Dict[str, object]:
    """Build drug_mapping_input.parquet — merged extended + openfda wide table.

    Unit: 1 row = 1 (safetyreportid, entry) drug record.
    This is the primary input for downstream RxNorm / ingredient mapping.

    Joins:
        drugcharacteristics_extended.parquet  (LEFT base)
        drug_openfda_wide.parquet             (LEFT enrichment)
    Key: (safetyreportid, entry)
    """
    extended_path = out_dir / "drugcharacteristics_extended.parquet"
    wide_path     = out_dir / "drug_openfda_wide.parquet"
    out_path      = out_dir / "drug_mapping_input.parquet"

    for p in (extended_path, wide_path):
        if not p.exists():
            raise FileNotFoundError(f"[S02] Missing prerequisite for drug_mapping_input: {p}")

    extended_lf = pl.scan_parquet(str(extended_path))
    wide_lf     = pl.scan_parquet(str(wide_path))

    merged_lf = extended_lf.join(
        wide_lf,
        on=["safetyreportid", "entry"],
        how="left",
    )

    rows = _write(merged_lf, out_path)
    logger.info("[S02-stream] drug_mapping_input rows=%d", rows)
    return {"rows": rows, "path": str(out_path)}


def _build_drug_mapping_input_unique(out_dir: Path) -> Dict[str, object]:
    """Build drug_mapping_input_unique.parquet — deduplicated mapping candidates.

    Unit: 1 row = 1 unique mapping record (collapsed from multiple source records).
    Adds two columns:
      - mapping_record_id:   sequential integer ID (1-based)
      - source_record_count: number of original (safetyreportid, entry) rows
                             that collapsed into this unique record

    Deduplication key (string-level fields + first element of list columns):
      medicinal_product, drug_administration, drug_dosage_form, drug_dosage_text,
      active_substance_name, drug_authorization_number,
      openfda_generic_name (first), openfda_brand_name (first),
      openfda_substance_name (first), openfda_application_number (first)

    Use this table to drive mapping jobs (LLM / RxNav) — avoids re-processing
    identical drug name strings thousands of times.
    """
    src_path = out_dir / "drug_mapping_input.parquet"
    out_path = out_dir / "drug_mapping_input_unique.parquet"

    if not src_path.exists():
        raise FileNotFoundError(f"[S02] Missing drug_mapping_input.parquet at {src_path}")

    df = pl.read_parquet(str(src_path))
    schema_names = df.schema.names()

    # Helper: extract first element from a List column for dedup key
    def _list_first_key(col: str, alias: str) -> pl.Expr:
        if col not in schema_names:
            return pl.lit(None, dtype=pl.Utf8).alias(alias)
        return (
            pl.when(
                pl.col(col).is_not_null()
                & (pl.col(col).list.len() > 0)
            )
            .then(pl.col(col).list.first())
            .otherwise(pl.lit(None, dtype=pl.Utf8))
            .cast(pl.Utf8, strict=False)
            .alias(alias)
        )

    def _str_key(col: str, alias: str) -> pl.Expr:
        if col not in schema_names:
            return pl.lit(None, dtype=pl.Utf8).alias(alias)
        return pl.col(col).cast(pl.Utf8, strict=False).fill_null("").alias(alias)

    # Build temporary dedup-key columns
    df = df.with_columns([
        _str_key("medicinal_product",        "_k_medicinal_product"),
        _str_key("drug_administration",       "_k_drug_administration"),
        _str_key("drug_dosage_form",          "_k_drug_dosage_form"),
        _str_key("drug_dosage_text",          "_k_drug_dosage_text"),
        _str_key("active_substance_name",     "_k_active_substance_name"),
        _str_key("drug_authorization_number", "_k_drug_authorization_number"),
        _list_first_key("openfda_generic_name",      "_k_openfda_generic_name"),
        _list_first_key("openfda_brand_name",         "_k_openfda_brand_name"),
        _list_first_key("openfda_substance_name",     "_k_openfda_substance_name"),
        _list_first_key("openfda_application_number", "_k_openfda_application_number"),
    ])

    dedup_key_cols = [c for c in df.columns if c.startswith("_k_")]

    # Count occurrences per unique key before deduplication
    count_df = (
        df
        .group_by(dedup_key_cols)
        .agg(pl.len().alias("source_record_count"))
    )

    # Deduplicate: keep first occurrence per key
    unique_df = df.unique(subset=dedup_key_cols, keep="first")

    # Join count back
    unique_df = unique_df.join(count_df, on=dedup_key_cols, how="left")

    # Drop temporary key columns
    unique_df = unique_df.drop(dedup_key_cols)

    # Add sequential mapping_record_id (1-based)
    unique_df = unique_df.with_row_index("mapping_record_id", offset=1)

    unique_df.write_parquet(str(out_path), compression="zstd")
    rows = unique_df.height
    logger.info("[S02-stream] drug_mapping_input_unique rows=%d", rows)
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
    logger.info("[S02-stream] reactions rows=%d", reaction_rows)
    return {"rows": reaction_rows, "path": str(reaction_path)}


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
    _cached = {
        "report":         _skip_if_exists(out_dir / "report.parquet",         "report.parquet"),
        "report_serious": _skip_if_exists(out_dir / "report_serious.parquet", "report_serious.parquet"),
        "reporter":       _skip_if_exists(out_dir / "reporter.parquet",       "reporter.parquet"),
    }
    if all(_cached.values()):
        manifest.update({k: v for k, v in _cached.items() if v})
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
    logger.success("[S02-stream] ER tables written to %s", out_dir)


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
    args = parser.parse_args()

    config = load_settings(args.config) if args.config else load_settings()
    ctx = init_run_context(config, run_id=args.run_id)
    run(ctx)
