from __future__ import annotations

"""Stage S03 – Join base tables and split adult/pediatric cohorts using Polars with early quality filters."""

from datetime import date
from pathlib import Path
from typing import Iterable

import polars as pl
from loguru import logger
from tqdm import tqdm

from src.utils.io import PipelineContext, stage_output_path, write_manifest

# Early quality filters (moved from S04 for better performance)
# NOTE: SUSPECT_DRUG_VALUE removed - now includes both suspect and concomitant drugs
EXCLUDED_REPORTERS = [
    "Unknown",
    "Lawyer",
    "Consumer or non-health professional"
]

# ============================================================================
# Date Filtering Configuration
# ============================================================================
# กำหนดช่วงเวลาที่ต้องการกรองข้อมูล (inclusive)
START_YEAR = 2014
END_YEAR = 2025


def _scan_required(ctx: PipelineContext, name: str, *, columns: Iterable[str]) -> pl.LazyFrame:
    """Load a required table from S02 Parquet if present; otherwise fall back to ER CSVs."""

    # Use local staging only (full_dataset self-contained, no parent references)
    local_parquet_path = stage_output_path(ctx, "s02_entity_format") / f"{name}.parquet"
    parquet_path = local_parquet_path if local_parquet_path.exists() else None

    if parquet_path:
        lf = pl.scan_parquet(parquet_path).select(list(columns))
    else:
        # Fallback to ER tables in CSV.GZ under metadata.er_tables_path (default data/er_tables)
        er_base = ctx.config.paths.root / str(ctx.config.metadata.get("er_tables_path", "data/er_tables"))
        csv_path = er_base / f"{name}.csv.gz"
        if not csv_path.exists():
            raise FileNotFoundError(
                f"Missing required table {name}. Checked:\n"
                f"  - {local_parquet_path}\n"
                f"  - {csv_path}\n"
                f"  Run S01 and S02 first to generate ER tables."
            )
        # Lazily scan CSV and select only required columns
        lf = (
            pl.scan_csv(
                csv_path,
                has_header=True,
                infer_schema_length=1000,
                ignore_errors=True,
                null_values=["", "NA", "NaN", "null", "NULL"],
            )
            .select(list(columns))
        )
    # Force `safetyreportid` to Utf8 to avoid implicit numeric promotion during joins.
    if "safetyreportid" in columns:
        lf = lf.with_columns(pl.col("safetyreportid").cast(pl.Utf8, strict=False))
    return lf


def _prepare_patient(ctx: PipelineContext) -> pl.LazyFrame:
    patient_columns = [
        "safetyreportid",
        "patient_custom_master_age",
        "patient_sex",
        "patient_weight",
    ]
    patient = _scan_required(ctx, "patient", columns=patient_columns)

    return (
        patient.with_columns(
            pl.col("patient_custom_master_age").cast(pl.Float64, strict=False),
            pl.col("patient_weight").cast(pl.Float64, strict=False),
        )
        .with_columns(
            pl.col("patient_custom_master_age").cast(pl.Int32, strict=False).alias("age_years")
        )
        .filter(pl.col("patient_custom_master_age").is_not_null())
    )


def _parse_date_column(col_name: str) -> pl.Expr:
    """
    Parse a date column with multiple format fallbacks.
    Supports: Date type, YYYYMMDD string, YYYY-MM-DD string.
    Returns null if parsing fails (equivalent to errors='coerce').
    """
    return pl.coalesce([
        pl.col(col_name).cast(pl.Date, strict=False),
        pl.col(col_name).cast(pl.Utf8, strict=False).str.strptime(pl.Date, format="%Y%m%d", strict=False),
        pl.col(col_name).cast(pl.Utf8, strict=False).str.strptime(pl.Date, format="%Y-%m-%d", strict=False),
    ]).alias(col_name)


def _prepare_report(ctx: PipelineContext) -> pl.LazyFrame:
    """
    Prepare report table with date parsing and index_date creation.
    
    Process:
    1. Parse all 3 date columns to proper Date type
    2. Create index_date using Waterfall Priority:
       - Priority 1: mostrecent_receive_date (latest update date)
       - Priority 2: lastupdate_date (system update date)
       - Priority 3: receive_date (initial receive date)
    3. Filter by date range [START_YEAR, END_YEAR]
    4. Sort by index_date ascending (oldest to newest)
    5. Keep original date columns for audit trail
    """
    report_columns = [
        "safetyreportid",
        "receive_date",
        "mostrecent_receive_date",
        "lastupdate_date",
    ]
    report = _scan_required(ctx, "report", columns=report_columns)
    
    # ─────────────────────────────────────────────────────────────────────────
    # Step 1: Parse all date columns to Date type (with error coercion)
    # ─────────────────────────────────────────────────────────────────────────
    report = report.with_columns([
        _parse_date_column("receive_date"),
        _parse_date_column("mostrecent_receive_date"),
        _parse_date_column("lastupdate_date"),
    ])
    
    # ─────────────────────────────────────────────────────────────────────────
    # Step 2: Create index_date using Waterfall Priority (vectorized coalesce)
    # Priority: mostrecent_receive_date → lastupdate_date → receive_date
    # ─────────────────────────────────────────────────────────────────────────
    report = report.with_columns(
        pl.coalesce([
            pl.col("mostrecent_receive_date"),  # Priority 1: Latest update
            pl.col("lastupdate_date"),          # Priority 2: System update
            pl.col("receive_date"),             # Priority 3: Initial receive
        ]).alias("index_date")
    )
    
    # ─────────────────────────────────────────────────────────────────────────
    # Step 3: Filter by date range [START_YEAR, END_YEAR]
    # - Remove rows with null index_date
    # - Keep only rows within the specified year range
    # ─────────────────────────────────────────────────────────────────────────
    start_date = date(START_YEAR, 1, 1)
    end_date = date(END_YEAR, 12, 31)
    
    report = report.filter(
        pl.col("index_date").is_not_null() &
        (pl.col("index_date") >= start_date) &
        (pl.col("index_date") <= end_date)
    )
    
    # ─────────────────────────────────────────────────────────────────────────
    # Step 4: Sort by index_date ascending (oldest to newest)
    # ─────────────────────────────────────────────────────────────────────────
    report = report.sort("index_date")
    
    # Note: Original date columns (receive_date, mostrecent_receive_date, 
    #       lastupdate_date) are preserved for audit/verification purposes
    
    return report


def _prepare_report_serious(ctx: PipelineContext) -> pl.LazyFrame:
    serious_columns = [
        "safetyreportid",
        "serious",
        "congenital_anomali",
        "death",
        "disabling",
        "hospitalization",
        "life_threatening",
        "other",
    ]
    lf = _scan_required(ctx, "report_serious", columns=serious_columns)
    return lf.with_columns(
        pl.col("congenital_anomali").cast(pl.Float64, strict=False),
        pl.col("death").cast(pl.Float64, strict=False),
        pl.col("disabling").cast(pl.Float64, strict=False),
        pl.col("hospitalization").cast(pl.Float64, strict=False),
        pl.col("life_threatening").cast(pl.Float64, strict=False),
        pl.col("other").cast(pl.Float64, strict=False),
    )


def _prepare_reporter(ctx: PipelineContext) -> pl.LazyFrame:
    reporter_columns = [
        "safetyreportid",
        "reporter_country",
        "reporter_company",
        "reporter_qualification",
    ]
    return _scan_required(ctx, "reporter", columns=reporter_columns)


def _prepare_drug(ctx: PipelineContext) -> pl.LazyFrame:
    drug_columns = [
        "safetyreportid",
        "medicinal_product",
        "drug_characterization",
        "drug_administration",
        "drug_indication",
    ]
    return _scan_required(ctx, "drugcharacteristics", columns=drug_columns).with_columns(
        pl.col("drug_indication").cast(pl.Utf8, strict=False)
    )


def _prepare_reaction(ctx: PipelineContext) -> pl.LazyFrame:
    reaction_columns = [
        "safetyreportid",
        "reaction_meddrapt",
        "reaction_outcome",
    ]
    return _scan_required(ctx, "reactions", columns=reaction_columns)


def _add_age_bands(patient: pl.LazyFrame) -> pl.LazyFrame:
    # age_years is already floored to whole years by S01 (np.floor), so we use
    # integer-range boundaries. term_neonatal (<28 days) and infancy (28 days–<1 yr)
    # both floor to 0 and are merged into a single "infancy" band.
    age = pl.col("patient_custom_master_age")

    nichd = (
        pl.when((age >= 0) & (age < 1)).then(pl.lit("infancy"))           # 0–11 months (neonatal + infancy)
        .when((age >= 1) & (age < 2)).then(pl.lit("toddler"))             # 1 year
        .when((age >= 2) & (age < 6)).then(pl.lit("early_childhood"))     # 2–5 years
        .when((age >= 6) & (age < 12)).then(pl.lit("middle_childhood"))   # 6–11 years
        .when((age >= 12) & (age < 18)).then(pl.lit("early_adolescence")) # 12–17 years
        .when((age >= 18) & (age <= 21)).then(pl.lit("late_adolescence")) # 18–21 years
        .otherwise(None)
        .alias("nichd")
    )

    return patient.with_columns(nichd)


def _build_pediatric(
    patient: pl.LazyFrame,
    report: pl.LazyFrame,
    report_serious: pl.LazyFrame,
    reporter: pl.LazyFrame,
    drug: pl.LazyFrame,
    reaction: pl.LazyFrame,
    *,
    output_path: Path,
) -> int:
    cutoff = 21.0
    # age >= 0 to include infants/neonates who floor to 0 after S01 conversion
    pediatric = patient.filter(
        (pl.col("patient_custom_master_age") >= 0) & (pl.col("patient_custom_master_age") <= cutoff)
    )
    pediatric = _add_age_bands(pediatric).filter(pl.col("nichd").is_not_null())

    joined = (
        pediatric
        .join(report, on="safetyreportid", how="inner")
        .join(report_serious, on="safetyreportid", how="inner")
        .join(reporter, on="safetyreportid", how="inner")
        .join(drug, on="safetyreportid", how="inner")
        .join(reaction, on="safetyreportid", how="inner")
        # Apply early quality filters
        .filter(
            # Basic validation
            pl.col("safetyreportid").is_not_null(),
            pl.col("reaction_meddrapt").is_not_null(),
            pl.col("medicinal_product").is_not_null(),
            pl.col("medicinal_product").str.len_chars() > 0,
            # Quality filters
            # NOTE: Removed suspect drug filter - now includes both suspect and concomitant drugs
            ~pl.col("reporter_qualification").is_in(EXCLUDED_REPORTERS),
        )
        .drop("patient_custom_master_age")
    )

    joined.sink_parquet(output_path, compression="zstd", statistics=True)

    count = (
        pl.scan_parquet(output_path)
        .select(pl.len())
        .collect(streaming=True)
        .item()
    )
    return int(count)


def _build_adult(
    patient: pl.LazyFrame,
    report: pl.LazyFrame,
    report_serious: pl.LazyFrame,
    reporter: pl.LazyFrame,
    drug: pl.LazyFrame,
    reaction: pl.LazyFrame,
    *,
    output_path: Path,
) -> int:
    cutoff = 21.0
    adult = patient.filter(
        (pl.col("patient_custom_master_age") > cutoff) & (pl.col("patient_custom_master_age") <= 120)
    )

    joined = (
        adult
        .join(report, on="safetyreportid", how="inner")
        .join(report_serious, on="safetyreportid", how="inner")
        .join(reporter, on="safetyreportid", how="inner")
        .join(drug, on="safetyreportid", how="inner")
        .join(reaction, on="safetyreportid", how="inner")
        # Apply early quality filters
        .filter(
            # Basic validation
            pl.col("safetyreportid").is_not_null(),
            pl.col("reaction_meddrapt").is_not_null(),
            pl.col("medicinal_product").is_not_null(),
            pl.col("medicinal_product").str.len_chars() > 0,
            # Quality filters
            # NOTE: Removed suspect drug filter - now includes both suspect and concomitant drugs
            ~pl.col("reporter_qualification").is_in(EXCLUDED_REPORTERS),
        )
        .drop("patient_custom_master_age")
    )

    joined.sink_parquet(output_path, compression="zstd", statistics=True)

    count = (
        pl.scan_parquet(output_path)
        .select(pl.len())
        .collect(streaming=True)
        .item()
    )
    return int(count)


def _build_cohort_drug_mapping_input(
    cohort: str,
    cohort_events_path: Path,
    drug_mapping_input_path: Path,
    output_dir: Path,
) -> int:
    """Build cohort-specific drug mapping input file.

    Unit: 1 row = 1 (safetyreportid, entry) drug record belonging to this cohort.

    Derives cohort membership from the already-built events file (fast scan of
    unique safetyreportids), then filters drug_mapping_input.parquet to only
    include drugs from reports in this cohort.

    Additionally enriches each row with patient context:
      - age_years               (from events)
      - nichd                   (pediatric only)
      - reporter_qualification  (from events)

    Output: {output_dir}/{cohort}_drug_mapping_input.parquet
    """
    out_path = output_dir / f"{cohort}_drug_mapping_input.parquet"

    if not cohort_events_path.exists():
        logger.warning("[S03] Events file missing for cohort %s — skipping drug mapping input", cohort)
        return 0

    if not drug_mapping_input_path.exists():
        logger.warning(
            "[S03] drug_mapping_input.parquet not found at %s — skipping cohort drug input. "
            "Run S02 with the new extended outputs first.",
            drug_mapping_input_path,
        )
        return 0

    # ── Step 1: Extract minimal patient context from the cohort events file ──
    # We only need per-report context columns (1 row per safetyreportid)
    patient_context_cols = ["safetyreportid", "age_years", "reporter_qualification"]
    if cohort == "pediatric":
        patient_context_cols.append("nichd")

    events_schema = pl.scan_parquet(str(cohort_events_path)).collect_schema().names()
    available_ctx = [c for c in patient_context_cols if c in events_schema]

    context_lf = (
        pl.scan_parquet(str(cohort_events_path))
        .select([pl.col(c) for c in available_ctx])
        .unique(subset=["safetyreportid"])  # 1 row per report (context is report-level)
    )

    # ── Step 2: Load drug_mapping_input and filter to cohort report IDs ──────
    # First collect unique safetyreportids for this cohort
    cohort_report_ids = (
        pl.scan_parquet(str(cohort_events_path))
        .select(pl.col("safetyreportid").cast(pl.Utf8, strict=False))
        .unique()
        .collect(streaming=True)
        .to_series()
        .to_list()
    )
    logger.info("[S03][%s] %d unique reports in cohort", cohort, len(cohort_report_ids))

    mapping_lf = (
        pl.scan_parquet(str(drug_mapping_input_path))
        .with_columns(pl.col("safetyreportid").cast(pl.Utf8, strict=False))
        .filter(pl.col("safetyreportid").is_in(cohort_report_ids))
    )

    # ── Step 3: Enrich with patient context ──────────────────────────────────
    enriched_lf = mapping_lf.join(
        context_lf,
        on="safetyreportid",
        how="left",
    )

    # Sink to parquet
    enriched_lf.sink_parquet(str(out_path), compression="zstd")

    rows = (
        pl.scan_parquet(str(out_path))
        .select(pl.len())
        .collect(streaming=True)
        .item()
    )
    logger.info("[S03][%s] drug_mapping_input rows=%d → %s", cohort, rows, out_path)
    return int(rows)


def run(ctx: PipelineContext) -> None:
    patient_lf = _prepare_patient(ctx)
    report_lf = _prepare_report(ctx)
    report_serious_lf = _prepare_report_serious(ctx)
    reporter_lf = _prepare_reporter(ctx)
    drug_lf = _prepare_drug(ctx)
    reaction_lf = _prepare_reaction(ctx)

    output_dir = stage_output_path(ctx, "s03_join_partition_age")
    output_dir.mkdir(parents=True, exist_ok=True)

    adult_path = output_dir / "adult_events_full_data.parquet"
    pediatric_path = output_dir / "pediatric_events_full_data.parquet"

    # ── Original cohort builds (backward-compatible) ──────────────────────────
    for _ in tqdm(range(1), desc="build pediatric cohort"):
        pass
    pediatric_count = _build_pediatric(
        patient_lf,
        report_lf,
        report_serious_lf,
        reporter_lf,
        drug_lf,
        reaction_lf,
        output_path=pediatric_path,
    )

    for _ in tqdm(range(1), desc="build adult cohort"):
        pass
    adult_count = _build_adult(
        patient_lf,
        report_lf,
        report_serious_lf,
        reporter_lf,
        drug_lf,
        reaction_lf,
        output_path=adult_path,
    )

    # ── New: cohort-specific drug mapping inputs ──────────────────────────────
    # Requires drug_mapping_input.parquet from S02 (new extended outputs).
    # Gracefully skips if S02 has not yet been re-run with the new outputs.
    s02_dir = stage_output_path(ctx, ctx.config.metadata.get("s02_stream_stage", "s02_entity_format"))
    drug_mapping_input_path = s02_dir / "drug_mapping_input.parquet"

    pediatric_drug_mapping_count = 0
    adult_drug_mapping_count = 0

    for _ in tqdm(range(1), desc="build pediatric drug mapping input"):
        pass
    pediatric_drug_mapping_count = _build_cohort_drug_mapping_input(
        cohort="pediatric",
        cohort_events_path=pediatric_path,
        drug_mapping_input_path=drug_mapping_input_path,
        output_dir=output_dir,
    )

    for _ in tqdm(range(1), desc="build adult drug mapping input"):
        pass
    adult_drug_mapping_count = _build_cohort_drug_mapping_input(
        cohort="adult",
        cohort_events_path=adult_path,
        drug_mapping_input_path=drug_mapping_input_path,
        output_dir=output_dir,
    )

    write_manifest(
        ctx,
        "s03_join_partition_age",
        {
            "stage": "s03_join_partition_age",
            "adult_rows": adult_count,
            "pediatric_rows": pediatric_count,
            "adult_output": str(adult_path),
            "pediatric_output": str(pediatric_path),
            # New outputs
            "adult_drug_mapping_input_rows": adult_drug_mapping_count,
            "pediatric_drug_mapping_input_rows": pediatric_drug_mapping_count,
            "adult_drug_mapping_input": str(output_dir / "adult_drug_mapping_input.parquet"),
            "pediatric_drug_mapping_input": str(output_dir / "pediatric_drug_mapping_input.parquet"),
        },
    )

    logger.success(
        "[S03] Cohorts generated — "
        "adult=%d, pediatric=%d | "
        "drug_mapping adult=%d, pediatric=%d",
        adult_count, pediatric_count,
        adult_drug_mapping_count, pediatric_drug_mapping_count,
    )


__all__ = ["run"]
