from __future__ import annotations

"""Stage S03 – Join base tables and split adult/pediatric cohorts using Polars with early quality filters.

Drug rows are read from ``drugcharacteristics_extended.parquet`` (S02) so each event carries
dosage / active substance / regulatory fields (the FAERS ``entry`` index is not written to
the cohort parquet — use ``medicinal_product`` and other drug columns). Requires S02 extended output;
re-run S02 then S03 if that file is missing.

Only **Suspect** drug lines are kept (``drug_characterization`` starts with ``Suspect``);
concomitant-only report lines are dropped at this stage.
"""

from datetime import date
from pathlib import Path
from typing import Iterable

import polars as pl
from loguru import logger
from tqdm import tqdm

from src.utils.io import PipelineContext, stage_output_path, write_manifest

# Early quality filters (moved from S04 for better performance)
# Keep only reporter-suspected drug lines (excludes concomitant / non-suspect).
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


def _suspect_drug_only() -> pl.Expr:
    """FAERS drug lines where the reporter marked the product as Suspect (not Concomitant)."""
    return pl.col("drug_characterization").cast(pl.Utf8, strict=False).str.starts_with("Suspect")


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
    Prepare report table with date parsing.

    Outputs only the three FAERS date fields (no combined ``index_date`` column):
    ``receive_date``, ``mostrecent_receive_date``, ``lastupdate_date``.

    Cohort inclusion still uses the same waterfall as the old index_date, but only
    as a transient key for filter/sort (not written to parquet):
    mostrecent_receive_date → lastupdate_date → receive_date.
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
    # Step 2: Transient timeline key (same priority as former index_date) for
    # filter + sort only — dropped before join; three source columns stay separate.
    # ─────────────────────────────────────────────────────────────────────────
    start_date = date(START_YEAR, 1, 1)
    end_date = date(END_YEAR, 12, 31)

    _timeline = pl.coalesce([
        pl.col("mostrecent_receive_date"),
        pl.col("lastupdate_date"),
        pl.col("receive_date"),
    ])
    report = report.with_columns(_timeline.alias("_timeline_key"))
    report = report.filter(
        pl.col("_timeline_key").is_not_null()
        & (pl.col("_timeline_key") >= pl.lit(start_date))
        & (pl.col("_timeline_key") <= pl.lit(end_date))
    )
    report = report.sort("_timeline_key").drop("_timeline_key")

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
    """Load per-(report, drug line) rows from S02 extended drug table.

    Each row in the source file is still one FAERS drug line; we omit ``entry`` from the
    selected columns so it does not appear in cohort outputs. Active substance, dosage,
    and regulatory fields are kept for downstream stages.
    """
    drug_columns = [
        "safetyreportid",
        "medicinal_product",
        "drug_characterization",
        "drug_administration",
        "drug_indication",
        "active_substance_name",
        "drug_dosage_form",
        "drug_authorization_number",
        "drug_batch_number",
        "drug_structured_dosage_num",
        "drug_structured_dosage_unit",
        "drug_interval_dosage_unit_num",
        "drug_interval_dosage_definition",
        "drug_treatment_duration",
        "drug_treatment_duration_unit",
        "drug_separate_dosage_num",
        "action_drug",
    ]
    lf = _scan_required(ctx, "drugcharacteristics_extended", columns=drug_columns)
    text_cols = [c for c in drug_columns if c != "safetyreportid"]
    return lf.with_columns([pl.col(c).cast(pl.Utf8, strict=False) for c in text_cols])


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
            # Exclude FAERS composite keys (e.g. 4816137-9); downstream expects digits-only IDs
            pl.col("safetyreportid").str.contains(r"^\d+$"),
            pl.col("reaction_meddrapt").is_not_null(),
            pl.col("medicinal_product").is_not_null(),
            pl.col("medicinal_product").str.len_chars() > 0,
            # Quality filters
            _suspect_drug_only(),
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
            # Exclude FAERS composite keys (e.g. 4816137-9); downstream expects digits-only IDs
            pl.col("safetyreportid").str.contains(r"^\d+$"),
            pl.col("reaction_meddrapt").is_not_null(),
            pl.col("medicinal_product").is_not_null(),
            pl.col("medicinal_product").str.len_chars() > 0,
            # Quality filters
            _suspect_drug_only(),
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
        logger.warning("[S03] Events file missing for cohort {} — skipping drug mapping input", cohort)
        return 0

    if not drug_mapping_input_path.exists():
        logger.warning(
            "[S03] drug_mapping_input.parquet not found at {} — skipping cohort drug input. "
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
    logger.info("[S03][{}] {} unique reports in cohort", cohort, len(cohort_report_ids))

    mapping_lf = (
        pl.scan_parquet(str(drug_mapping_input_path))
        .with_columns(pl.col("safetyreportid").cast(pl.Utf8, strict=False))
        .filter(pl.col("safetyreportid").is_in(cohort_report_ids))
    )
    map_schema = pl.scan_parquet(str(drug_mapping_input_path)).collect_schema().names()
    if "drug_characterization" in map_schema:
        mapping_lf = mapping_lf.filter(
            pl.col("drug_characterization").cast(pl.Utf8, strict=False).str.starts_with("Suspect")
        )
    else:
        logger.warning(
            "[S03][{}] drug_mapping_input has no drug_characterization — cannot apply suspect-only filter",
            cohort,
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
    logger.info("[S03][{}] drug_mapping_input rows={} → {}", cohort, rows, out_path)
    return int(rows)


def _validate_cohort_drug_mapping(path: Path, cohort: str) -> dict:
    """Validate cohort drug_mapping_input: row count, duplicate check, non-null coverage."""
    if not path.exists():
        return {}
    try:
        df = pl.scan_parquet(str(path))
        total = df.select(pl.len()).collect().item()
        if total == 0:
            return {"rows": 0, "duplicate_safetyreportid_entry_pairs": 0, "coverage_pct": {}}

        dup_count = (
            df.group_by(["safetyreportid", "entry"])
            .agg(pl.len().alias("_n"))
            .filter(pl.col("_n") > 1)
            .select(pl.len())
            .collect()
            .item()
        )

        key_cols = [
            "medicinal_product",
            "active_substance_name",
            "openfda_rxcui",
            "openfda_generic_name",
            "openfda_substance_name",
        ]
        schema = pl.scan_parquet(str(path)).collect_schema()
        coverage: dict = {}
        for col in key_cols:
            if col not in schema.names():
                continue
            dtype = schema[col]
            is_list = hasattr(dtype, "inner")  # List types have .inner attribute
            if is_list:
                nn = (
                    df.filter(pl.col(col).is_not_null() & (pl.col(col).list.len() > 0))
                    .select(pl.len())
                    .collect()
                    .item()
                )
            else:
                nn = (
                    df.filter(
                        pl.col(col).is_not_null()
                        & (pl.col(col).cast(pl.Utf8, strict=False).str.len_chars() > 0)
                    )
                    .select(pl.len())
                    .collect()
                    .item()
                )
            coverage[col] = round(nn / total * 100, 2) if total else 0

        if dup_count > 0:
            logger.warning("[S03][{}] {} duplicate (safetyreportid, entry) in drug_mapping_input", cohort, dup_count)
        return {
            "rows": total,
            "duplicate_safetyreportid_entry_pairs": dup_count,
            "coverage_pct": coverage,
        }
    except Exception as e:
        logger.warning("[S03] Validation failed for {}: {}", path, e)
        return {"error": str(e)}


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

    # Validation for cohort drug mapping inputs
    adult_drug_mapping_path = output_dir / "adult_drug_mapping_input.parquet"
    pediatric_drug_mapping_path = output_dir / "pediatric_drug_mapping_input.parquet"
    adult_validation = _validate_cohort_drug_mapping(adult_drug_mapping_path, "adult")
    pediatric_validation = _validate_cohort_drug_mapping(pediatric_drug_mapping_path, "pediatric")

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
            "adult_drug_mapping_input": str(adult_drug_mapping_path),
            "pediatric_drug_mapping_input": str(pediatric_drug_mapping_path),
            "validation": {
                "adult_drug_mapping_input": adult_validation,
                "pediatric_drug_mapping_input": pediatric_validation,
            },
        },
    )

    logger.success(
        "[S03] Cohorts generated — "
        "adult={}, pediatric={} | "
        "drug_mapping adult={}, pediatric={}",
        adult_count, pediatric_count,
        adult_drug_mapping_count, pediatric_drug_mapping_count,
    )


__all__ = ["run"]
