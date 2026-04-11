from __future__ import annotations

"""Stage S10 – Package deliverable tables for distribution (disk-friendly Polars)."""

from pathlib import Path
from typing import Dict

import polars as pl
from loguru import logger

from src.utils.io import PipelineContext, output_dataset_path, stage_output_path


def _format_value(value: object) -> str:
    if isinstance(value, int):
        return f"{value:,}"
    if isinstance(value, float):
        return f"{value:,.2f}"
    if isinstance(value, Path):
        return str(value)
    return str(value)


def _log_box(stage: str, title: str, **fields: object) -> None:
    lines = [f"{name}: {_format_value(value)}" for name, value in fields.items()]
    width = max(len(title), *(len(line) for line in lines)) if lines else len(title)
    border = "─" * (width + 2)
    prefix = f"[S10][{stage}] "
    rows = [
        prefix + "┌" + border + "┐",
        prefix + "│ " + title.ljust(width) + " │",
    ]
    if lines:
        rows.append(prefix + "├" + "─" * (width + 2) + "┤")
        rows.extend(prefix + "│ " + line.ljust(width) + " │" for line in lines)
    rows.append(prefix + "└" + border + "┘")
    logger.info("\n" + "\n".join(rows))


def _cohort_title(cohort: str) -> str:
    return "Adult" if cohort == "adult" else "Pediatric"


def _package_cohort_pl(ctx: PipelineContext, cohort: str) -> Dict[str, int]:
    """Package deliverable tables from S09 final output.
    
    Updated to work with new S06 dictionary-based approach.
    All data comes from patient_report_reporter_drug_reaction_full_data.parquet.
    """
    s09_output = output_dataset_path(ctx, cohort, "patient_report_reporter_drug_reaction_full_data.parquet")

    if not s09_output.exists():
        raise FileNotFoundError(f"Missing patient report for cohort {cohort}: {s09_output}")

    cohort_dir = (ctx.config.paths.output_root / _cohort_title(cohort)).resolve()
    cohort_dir.mkdir(parents=True, exist_ok=True)

    counts: Dict[str, int] = {}

    # All data comes from S09 final output
    pr = pl.scan_parquet(str(s09_output))

    # patient_report count
    counts["patient_report"] = int(pr.select(pl.len()).collect(streaming=True).item())
    _log_box(cohort, "Patient report", rows=counts["patient_report"], path=s09_output)

    # drug.parquet: one row per RxCUI (canonical drug entity for counts / ROR)
    drug_lf = (
        pr
        .filter(pl.col("rxcui").is_not_null())
        .group_by("rxcui")
        .agg(
            pl.col("ingredient").first().alias("ingredient"),
            pl.col("medicinal_product").first().alias("medicinal_product"),
            pl.col("mapping_method").first().alias("mapping_method"),
        )
        .select("rxcui", "ingredient", "medicinal_product", "mapping_method")
    )
    drug_path = cohort_dir / "drug_full_data.parquet"
    counts["drug"] = int(drug_lf.select(pl.len()).collect(streaming=True).item())
    drug_lf.sink_parquet(str(drug_path), compression="zstd")
    _log_box(cohort, "Deliverable", name="drug_full_data.parquet", rows=counts["drug"], path=drug_path)

    # adr.parquet: per-report reactions with MedDRA mapping
    # All MedDRA fields already present in S09 output
    # Note: meddra_soc_names/codes are Lists (PT can have multiple SOCs)
    # Keep as List to preserve all SOC information
    adr_lf = (
        pr
        .filter(pl.col("meddra_concept_id").is_not_null())
        .group_by(["safetyreportid", "reaction_meddrapt"])
        .agg(
            pl.col("reaction_outcome").first().alias("reaction_outcome"),
            pl.col("meddra_concept_id").first().alias("meddra_concept_id"),
            pl.col("meddra_concept_code").first().alias("meddra_concept_code"),
            pl.col("meddra_soc_names").first().alias("meddra_soc_names"),  # Keep as List
            pl.col("meddra_soc_codes").first().alias("meddra_soc_codes"),  # Keep as List
        )
        .select(
            "safetyreportid",
            "reaction_meddrapt",
            "reaction_outcome",
            "meddra_concept_id",
            "meddra_concept_code",
            "meddra_soc_names",
            "meddra_soc_codes",
        )
    )
    adr_path = cohort_dir / "adr_full_data.parquet"
    counts["adr"] = int(adr_lf.select(pl.len()).collect(streaming=True).item())
    adr_lf.sink_parquet(str(adr_path), compression="zstd")
    _log_box(cohort, "Deliverable", name="adr_full_data.parquet", rows=counts["adr"], path=adr_path)

    # standard_reaction.parquet: per-report standardized reactions with SOC
    # Note: meddra_soc_names/codes are Lists (PT can have multiple SOCs)
    # Keep as List to preserve all SOC information
    std_react_lf = (
        pr
        .filter(pl.col("meddra_concept_id").is_not_null())
        .group_by(["safetyreportid", "meddra_concept_id"])  # enforce uniqueness on (report, concept)
        .agg(
            pl.col("meddra_concept_code").first().alias("MedDRA_concept_code"),
            pl.col("reaction_meddrapt").first().alias("MedDRA_concept_name"),  # Use original term
            pl.col("reaction_outcome").first().alias("reaction_outcome"),
            pl.col("meddra_soc_names").first().alias("MedDRA_soc_names"),  # Keep as List
            pl.col("meddra_soc_codes").first().alias("MedDRA_soc_codes"),  # Keep as List
        )
        .with_columns(
            pl.col("meddra_concept_id").cast(pl.Int64).alias("MedDRA_concept_id"),
            pl.col("safetyreportid").cast(pl.Utf8, strict=False),
        )
        .select(
            "safetyreportid",
            "MedDRA_concept_id",
            "MedDRA_concept_code",
            "MedDRA_concept_name",
            "MedDRA_soc_names",
            "MedDRA_soc_codes",
            "reaction_outcome",
        )
    )
    std_react_path = cohort_dir / "standard_reaction_full_data.parquet"
    counts["standard_reaction"] = int(std_react_lf.select(pl.len()).collect(streaming=True).item())
    std_react_lf.sink_parquet(str(std_react_path), compression="zstd")
    _log_box(cohort, "Deliverable", name="standard_reaction_full_data.parquet", rows=counts["standard_reaction"], path=std_react_path)

    return counts


def run(ctx: PipelineContext) -> None:
    _log_box("summary", "Packaging deliverables", status="start")
    results: Dict[str, Dict[str, int]] = {}
    for cohort in ("pediatric", "adult"):
        counts = _package_cohort_pl(ctx, cohort)
        results[cohort] = counts
    _log_box("summary", "Packaging complete", cohorts=list(results.keys()))


__all__ = ["run"]


