from __future__ import annotations

"""Stage S09 – Final merge, filtering, and reporting outputs.

Filtering criteria (moved from S09 for early filtering):
1. Suspect drugs only (drug_characterization) - MOVED TO S03
2. Qualified reporters only (exclude Unknown, Lawyer, Consumer) - MOVED TO S03

Additional filtering in S09:
3. Valid ingredient and medicinal_product (NOTE: For full_dataset, no ingredient_count or rxcui_tty filtering)
4. MedDRA concept mapped
5. ``receive_date`` (parsed) must fall in ``[START_YEAR, END_YEAR]`` inclusive (same bounds as S03 timeline filter; S09 applies explicitly to ``receive_date``)

Drug columns for downstream handoff (stratify analyses on ``rxcui``):
- ``ingredient``: ``List[str]`` — RxNorm related ingredients for the mapped RxCUI (canonical per concept).
- ``faers_ingredients``: ``List[str]`` — structured ingredients parsed from FAERS/S07b (provenance).
- ``ingredient_count`` / ``faers_ingredient_count``: list lengths; ``is_combination_product`` if either list length > 1.
"""

from datetime import date
from pathlib import Path
from typing import Any, Dict, List

import polars as pl
from loguru import logger

from src.stages.s03_join_partition_age import END_YEAR, START_YEAR
from src.utils.constants import EXCLUDED_REPORTERS
from src.utils.dates import parse_date_column
from src.utils.dq import dq_summary_markdown
from src.utils.io import (
    PipelineContext,
    output_dataset_path,
    stage_output_path,
    write_manifest,
)


ADULT_COLUMNS = [
    "safetyreportid",
    "age_years",
    "patient_sex",
    "receive_date",
    "mostrecent_receive_date",
    "lastupdate_date",
    "serious",
    "congenital_anomali",
    "death",
    "disabling",
    "hospitalization",
    "life_threatening",
    "other",
    "reporter_country",
    "reporter_company",
    "reporter_qualification",
    "medicinal_product",
    "rxcui",
    "mapping_method",  # Method used to resolve rxcui (rxnav_direct, local_synonym, cid_title_fallback)
    "ingredient",      # List[str] — RxNorm ingredients for this rxcui (use rxcui to count drugs)
    "faers_ingredients",   # List[str] from FAERS/S07b structured parsing
    "ingredient_count",
    "faers_ingredient_count",
    "is_combination_product",
    "drug_characterization",
    "drug_administration",
    "drug_indication",
    "reaction_meddrapt",
    "reaction_outcome",
    "meddra_concept_id",
    "meddra_concept_code",
    "meddra_soc_codes",    # List of SOC codes (PT can have multiple SOCs)
    "meddra_soc_names",    # List of SOC names (PT can have multiple SOCs)
]


PEDIATRIC_COLUMNS = [
    "safetyreportid",
    "age_years",
    "patient_sex",
    "nichd",
    "receive_date",
    "mostrecent_receive_date",
    "lastupdate_date",
    "serious",
    "congenital_anomali",
    "death",
    "disabling",
    "hospitalization",
    "life_threatening",
    "other",
    "reporter_country",
    "reporter_company",
    "reporter_qualification",
    "medicinal_product",
    "rxcui",
    "mapping_method",  # Method used to resolve rxcui (rxnav_direct, local_synonym, cid_title_fallback)
    "ingredient",      # List[str] — RxNorm ingredients for this rxcui (use rxcui to count drugs)
    "faers_ingredients",   # List[str] from FAERS/S07b structured parsing
    "ingredient_count",
    "faers_ingredient_count",
    "is_combination_product",
    "drug_characterization",
    "drug_administration",
    "drug_indication",
    "reaction_meddrapt",
    "reaction_outcome",
    "meddra_concept_id",
    "meddra_concept_code",
    "meddra_soc_codes",    # List of SOC codes (PT can have multiple SOCs)
    "meddra_soc_names",    # List of SOC names (PT can have multiple SOCs)
]


FILL_UNKNOWN_COLUMNS = [
    "patient_sex",
    "reaction_outcome",
    "drug_administration",
    "drug_indication",
    "reporter_country",
    "reporter_company",
    "reporter_qualification",
    "Drug characterization",
]

# EXCLUDED_REPORTERS imported from src.utils.constants


def _receive_date_bounds() -> tuple[date, date]:
    return date(START_YEAR, 1, 1), date(END_YEAR, 12, 31)


def _filter_receive_date_in_window() -> pl.Expr:
    """Parsed ``receive_date`` in [START_YEAR, END_YEAR] inclusive; excludes null dates."""
    d0, d1 = _receive_date_bounds()
    return (
        pl.col("receive_date").is_not_null()
        & (pl.col("receive_date") >= pl.lit(d0))
        & (pl.col("receive_date") <= pl.lit(d1))
    )


def _empty_list_utf8() -> pl.Expr:
    return pl.lit([], dtype=pl.List(pl.Utf8))


def _drug_lists_exprs(schema: pl.Schema) -> list[pl.Expr]:
    """RxNorm list as ``ingredient`` + FAERS structured list (second with_columns adds counts)."""
    names = schema.names()
    dtypes = {n: schema.get(n) for n in names}

    if "rxnorm_ingredients" in names:
        rxnorm = (
            pl.when(pl.col("rxnorm_ingredients").is_not_null())
            .then(pl.col("rxnorm_ingredients"))
            .otherwise(_empty_list_utf8())
            .alias("ingredient")
        )
    elif "ingredients_right" in names:
        # Legacy S08: RxNorm list before rename to rxnorm_ingredients
        rxnorm = (
            pl.when(pl.col("ingredients_right").is_not_null())
            .then(pl.col("ingredients_right"))
            .otherwise(_empty_list_utf8())
            .alias("ingredient")
        )
    else:
        rxnorm = _empty_list_utf8().alias("ingredient")

    if "ingredients" in names:
        dt = dtypes.get("ingredients")
        dt_str = str(dt) if dt is not None else ""
        if dt_str.startswith("List"):
            faers = (
                pl.when(pl.col("ingredients").is_not_null())
                .then(pl.col("ingredients"))
                .otherwise(_empty_list_utf8())
                .alias("faers_ingredients")
            )
        else:
            faers = (
                pl.when(
                    pl.col("ingredients").is_not_null()
                    & (pl.col("ingredients").cast(pl.Utf8, strict=False).str.len_chars() > 0)
                )
                .then(
                    pl.col("ingredients")
                    .cast(pl.Utf8, strict=False)
                    .str.split(";")
                    .list.eval(pl.element().str.strip_chars())
                    .list.drop_nulls()
                )
                .otherwise(_empty_list_utf8())
                .alias("faers_ingredients")
            )
    else:
        faers = _empty_list_utf8().alias("faers_ingredients")

    return [rxnorm, faers]


def _drug_count_exprs() -> list[pl.Expr]:
    """Depends on columns created by ``_drug_lists_exprs``."""
    return [
        pl.col("ingredient").list.len().cast(pl.Int32).alias("ingredient_count"),
        pl.col("faers_ingredients").list.len().cast(pl.Int32).alias("faers_ingredient_count"),
        (
            (pl.col("ingredient").list.len() > 1) | (pl.col("faers_ingredients").list.len() > 1)
        ).alias("is_combination_product"),
    ]


def _format_value(value: Any) -> str:
    if isinstance(value, int):
        return f"{value:,}"
    if isinstance(value, float):
        return f"{value:,.2f}"
    if isinstance(value, Path):
        return str(value)
    return str(value)


def _log_box(cohort: str, title: str, **fields: Any) -> None:
    lines = [f"{name}: {_format_value(value)}" for name, value in fields.items()]
    width = max(len(title), *(len(line) for line in lines)) if lines else len(title)
    border = "─" * (width + 2)
    prefix = f"[S09][{cohort}] "
    rows = [
        prefix + "┌" + border + "┐",
        prefix + "│ " + title.ljust(width) + " │",
    ]
    if lines:
        rows.append(prefix + "├" + "─" * (width + 2) + "┤")
        rows.extend(
            prefix + "│ " + line.ljust(width) + " │"
            for line in lines
        )
    rows.append(prefix + "└" + border + "┘")
    logger.info("\n" + "\n".join(rows))


def _apply_meddra_filters(frame: pl.LazyFrame) -> pl.LazyFrame:
    lf = frame
    names = lf.collect_schema().names()
    if "meddra_concept_id" in names:
        lf = lf.filter(pl.col("meddra_concept_id").is_not_null())
    return lf


# NOTE: Two legacy implementations (_finalize_cohort_streaming and _finalize_cohort_duckdb)
# were removed in this refactoring. Only the batched Polars implementation is used.
# See git history for the removed code if needed.

def _finalize_cohort_polars_batched(cohort: str, ctx: PipelineContext) -> Dict[str, int]:
    clean_dir = stage_output_path(ctx, "s03_join_partition_age")
    enrich_dir = stage_output_path(ctx, "s08_enrich_drug_identifiers")
    mapping_dir = stage_output_path(ctx, "s06_map_omop_meddra")

    clean_path = clean_dir / f"{cohort}_events_full_data.parquet"
    _enriched_final = enrich_dir / f"{cohort}_drugs_enriched_final_full_data.parquet"
    enriched_path = _enriched_final if _enriched_final.exists() else enrich_dir / f"{cohort}_drugs_enriched.parquet"
    dict_path = mapping_dir / cohort / "pt_soc_dictionary_full_data.parquet"

    for path in (clean_path, enriched_path, dict_path):
        if not path.exists():
            raise FileNotFoundError(path)

    clean_rows = (
        pl.scan_parquet(str(clean_path)).select(pl.len()).collect(streaming=True).item()
    )
    # Inspect schema to check available columns
    enriched_schema = pl.scan_parquet(str(enriched_path)).collect_schema()
    has_is_selected_case = "is_selected_case" in enriched_schema

    base_lf_stats = pl.scan_parquet(str(enriched_path))
    if not has_is_selected_case:
        base_lf_stats = base_lf_stats.with_columns(
            pl.col("rxcui").is_not_null().cast(pl.Boolean).alias("is_selected_case")
        )
    else:
        base_lf_stats = base_lf_stats.with_columns(
            pl.col("is_selected_case").fill_null(pl.col("rxcui").is_not_null()).cast(pl.Boolean).alias("is_selected_case")
        )

    enriched_unique = (
        base_lf_stats
        .filter(pl.col("is_selected_case") == True)
        .filter(pl.col("rxcui").is_not_null())
        .select(pl.col("rxcui").n_unique())
        .collect(streaming=True)
        .item()
    )

    _log_box(
        cohort,
        "Prepare lazy pipeline",
        clean_rows=clean_rows,
        candidate_drugs=enriched_unique,
    )

    target_columns = ADULT_COLUMNS if cohort == "adult" else PEDIATRIC_COLUMNS

    clean_lf = (
        pl.scan_parquet(str(clean_path))
        .with_columns(
            pl.col("medicinal_product").cast(pl.Utf8, strict=False),
            pl.col("safetyreportid").cast(pl.Utf8, strict=False),
            pl.col("reaction_meddrapt").cast(pl.Utf8, strict=False),
        )
    )
    # S08 final → ``ingredient`` = List[str] (RxNorm), ``faers_ingredients`` = FAERS structured
    enriched_schema_lookup = pl.scan_parquet(str(enriched_path)).collect_schema()
    has_is_selected_case_lookup = "is_selected_case" in enriched_schema_lookup
    has_mapping_method_lookup = "mapping_method" in enriched_schema_lookup
    has_source_lookup = "source" in enriched_schema_lookup
    has_lookup_hit = "lookup_hit" in enriched_schema_lookup

    # Build is_selected_case and mapping_method columns based on schema availability
    base_lf = pl.scan_parquet(str(enriched_path))

    # Build is_selected_case column
    if not has_is_selected_case_lookup:
        base_lf = base_lf.with_columns(
            pl.col("rxcui").is_not_null().cast(pl.Boolean).alias("is_selected_case")
        )
    else:
        base_lf = base_lf.with_columns(
            pl.col("is_selected_case").fill_null(pl.col("rxcui").is_not_null()).cast(pl.Boolean).alias("is_selected_case")
        )

    # Build mapping_method column (prefer lookup_hit from S08 if available)
    if has_mapping_method_lookup:
        base_lf = base_lf.with_columns(pl.col("mapping_method").cast(pl.Utf8).alias("mapping_method"))
    elif has_lookup_hit:
        base_lf = base_lf.with_columns(pl.col("lookup_hit").cast(pl.Utf8).alias("mapping_method"))
    elif has_source_lookup:
        base_lf = base_lf.with_columns(pl.col("source").cast(pl.Utf8).alias("mapping_method"))
    else:
        base_lf = base_lf.with_columns(pl.lit("enriched").cast(pl.Utf8).alias("mapping_method"))

    enriched_lookup_lf = (
        base_lf
        .with_columns(_drug_lists_exprs(enriched_schema_lookup))
        .with_columns(_drug_count_exprs())
    )

    enriched_lookup_lf = (
        enriched_lookup_lf
        .select(
            pl.col("medicinal_product").cast(pl.Utf8),
            pl.col("rxcui"),
            pl.col("ingredient"),
            pl.col("mapping_method").cast(pl.Utf8),
            pl.col("is_selected_case").cast(pl.Boolean),
            pl.col("faers_ingredients"),
            pl.col("ingredient_count"),
            pl.col("faers_ingredient_count"),
            pl.col("is_combination_product"),
        )
        .filter(pl.col("is_selected_case") == True)
        .drop_nulls("medicinal_product")
        .filter(pl.col("rxcui").is_not_null())
        .filter(
            (pl.col("ingredient").list.len() > 0) | (pl.col("faers_ingredients").list.len() > 0)
        )
        .unique(subset=["medicinal_product"], keep="first")
    )
    # Load PT->SOC dictionary (Title Case mapping from Stage 6)
    soc_dict_df = pl.read_parquet(str(dict_path))
    logger.info(
        f"[S09][{cohort}] Loaded PT-SOC dictionary: {soc_dict_df.height} unique PT terms"
    )
    
    logger.info(
        f"[S09][{cohort}] Quality filters: "
        f"receive_date in [{START_YEAR},{END_YEAR}], "
        f"Suspect drugs only, "
        f"Excluded reporters: {', '.join(EXCLUDED_REPORTERS)}"
    )

    # Build batched list of medicinal_product to process
    product_values = (
        enriched_lookup_lf
        .select(pl.col("medicinal_product").unique())
        .collect(streaming=True)
        .to_series()
        .to_list()
    )
    batch_size = int(getattr(ctx.config, "metadata", {}).get("finalize_product_batch_size", 1000))
    total_batches = (len(product_values) + max(batch_size, 1) - 1) // max(batch_size, 1)

    # Optional tqdm progress bar
    pbar = None
    try:
        from tqdm import tqdm as _tqdm  # type: ignore
        pbar = _tqdm(total=total_batches, desc=f"S09 {cohort} batches", leave=False)
    except Exception:
        pbar = None

    tmp_root = stage_output_path(ctx, "s09_finalize_merge_and_report")
    tmp_root.mkdir(parents=True, exist_ok=True)
    cohort_dir = tmp_root / cohort
    cohort_dir.mkdir(parents=True, exist_ok=True)
    # Clean previous chunk files for this cohort only
    for p in cohort_dir.glob("batch_*.parquet"):
        try:
            p.unlink()
        except Exception:
            pass

    # Process in batches
    for i in range(0, len(product_values), batch_size):
        chunk = product_values[i : i + batch_size]
        if not chunk:
            continue
        chunk_path = cohort_dir / f"batch_{i//batch_size:05d}.parquet"

        # Join clean + enriched, then normalize reaction_meddrapt to Title Case
        base_join = (
            clean_lf
            .filter(pl.col("medicinal_product").is_in(chunk))
            .join(enriched_lookup_lf, on="medicinal_product", how="inner")
            .with_columns(
                # Normalize reaction_meddrapt to Title Case (overwrite)
                pl.col("reaction_meddrapt")
                .cast(pl.Utf8, strict=False)
                .str.to_titlecase()
                .alias("reaction_meddrapt")  # Overwrite original
            )
        )
        
        # Join with PT-SOC dictionary using Title Case matching
        joined = (
            base_join
            .join(
                soc_dict_df.lazy(),
                left_on="reaction_meddrapt",  # Use normalized column directly
                right_on="MedDRA_concept_name",
                how="inner"
            )
        )

        processed = joined.with_columns(
            parse_date_column("receive_date"),
        )

        # Apply quality filters (Note: drug_characterization and reporter filters moved to S03)
        processed = (
            processed
            .filter(_filter_receive_date_in_window())
            # ✅ is_selected_case = mapping_success only (no ingredient_count or rxcui_tty filtering for full_dataset)
            .filter(pl.col("medicinal_product").is_not_null())
            # Suspect-only drugs: applied in S03
            # Filter: Exclude unwanted reporters
            .filter(~pl.col("reporter_qualification").is_in(EXCLUDED_REPORTERS))
        )

        seriousness_flags = [
            "congenital_anomali",
            "disabling",
            "life_threatening",
            "death",
            "other",
            "hospitalization",
        ]
        names_after_join = processed.collect_schema().names()
        available_flags = [c for c in seriousness_flags if c in names_after_join]
        if available_flags:
            processed = processed.with_columns([
                pl.when(pl.col(col).cast(pl.Int64, strict=False) == 1).then(1).otherwise(0).cast(pl.Int32).alias(col)
                for col in available_flags
            ])

        serious_text = (
            pl.col("serious").cast(pl.Utf8, strict=False).str.to_lowercase()
            if "serious" in names_after_join
            else None
        )
        serious_numeric = (
            pl.when(pl.col("serious").cast(pl.Int64, strict=False) == 1).then(1)
            .when(pl.col("serious").cast(pl.Int64, strict=False) == 2).then(0)
            .when(serious_text.str.contains("did not result in any of the above", literal=True)).then(0)
            .when(serious_text.str.contains("resulted in", literal=False)).then(1)
            .otherwise(0)
            .cast(pl.Int32)
            if serious_text is not None
            else pl.lit(0, dtype=pl.Int32)
        )
        processed = processed.with_columns(serious_numeric.alias("_serious_numeric"))
        processed = processed.with_columns(
            pl.max_horizontal([pl.col(c) for c in available_flags] + [pl.col("_serious_numeric")]).cast(pl.Int32).alias("serious")
        ).drop("_serious_numeric")

        if "mostrecent_receive_date" in names_after_join:
            processed = processed.with_columns(
                parse_date_column("mostrecent_receive_date")
            ).drop_nulls(["mostrecent_receive_date"])

        if "lastupdate_date" in names_after_join:
            processed = processed.with_columns(parse_date_column("lastupdate_date"))

        if "patient_weight" in names_after_join:
            processed = processed.drop("patient_weight")
        if "rxcui_tty" in names_after_join:
            processed = processed.drop("rxcui_tty")  # Helper column used for filtering
        if "ingredients" in names_after_join:
            processed = processed.drop("ingredients")

        existing_fill = [c for c in FILL_UNKNOWN_COLUMNS if c in names_after_join]
        if existing_fill:
            processed = processed.with_columns([
                pl.col(col).fill_null("Unknown").alias(col)
                for col in existing_fill
            ])

        if cohort == "adult" and "nichd" in names_after_join:
            processed = processed.drop("nichd")

        processed = _apply_meddra_filters(processed)

        # ─── Deduplicate: 1 row per (report, drug, reaction) ─────────────────
        # FAERS source data can have the same drug listed multiple times in one
        # report (e.g., as both Suspect AND Concomitant, or with different
        # dosage routes).  We keep the most informative characterisation:
        #   Suspect  (priority 0) > Interacting (1) > Concomitant (2) > other (3)
        dedup_keys = ["safetyreportid", "medicinal_product", "reaction_meddrapt"]
        final_names_before_dedup = processed.collect_schema().names()
        dedup_keys_present = [c for c in dedup_keys if c in final_names_before_dedup]
        if len(dedup_keys_present) == 3:
            if "drug_characterization" in final_names_before_dedup:
                processed = (
                    processed
                    .with_columns(
                        pl.when(pl.col("drug_characterization").str.starts_with("Suspect")).then(pl.lit(0))
                        .when(pl.col("drug_characterization").str.starts_with("Interacting")).then(pl.lit(1))
                        .when(pl.col("drug_characterization").str.starts_with("Concomitant")).then(pl.lit(2))
                        .otherwise(pl.lit(3))
                        .cast(pl.Int8)
                        .alias("_char_prio")
                    )
                    .sort("_char_prio", nulls_last=True)
                    .unique(subset=dedup_keys_present, keep="first", maintain_order=False)
                    .drop("_char_prio")
                )
            else:
                processed = processed.unique(subset=dedup_keys_present, keep="first", maintain_order=False)

        # Select only target columns (this automatically excludes helper columns)
        final_names = processed.collect_schema().names()
        final_chunk = processed.select([c for c in target_columns if c in final_names])

        # Write chunk parquet
        final_chunk.sink_parquet(str(chunk_path), compression="zstd")
        if pbar is not None:
            try:
                pbar.update(1)
            except Exception:
                pass

    # Close progress bar if used
    if pbar is not None:
        try:
            pbar.close()
        except Exception:
            pass

    # Merge chunk outputs into final file
    out_events = output_dataset_path(ctx, cohort, "patient_report_reporter_drug_reaction_full_data.parquet")
    chunk_files = sorted(cohort_dir.glob("batch_*.parquet"))
    if not chunk_files:
        raise RuntimeError("No chunk outputs produced for cohort " + cohort)
    _log_box(cohort, "Write parquet", action="start", path=out_events)
    combined_lf = pl.scan_parquet([str(p) for p in chunk_files])
    combined_lf.sink_parquet(str(out_events), compression="zstd")
    _log_box(cohort, "Write parquet", status="complete")

    # Stats
    out_scan = pl.scan_parquet(str(out_events))
    _log_box(cohort, "Collect stats")
    output_rows = out_scan.select(pl.len()).collect(streaming=True).item()
    mapped_rows = (
        out_scan.filter(pl.col("rxcui").is_not_null()).select(pl.len()).collect(streaming=True).item()
        if output_rows else 0
    )
    missing_rows = output_rows - mapped_rows
    unique_after_schema = out_scan.collect_schema().names()
    unique_after = (
        out_scan.select(pl.col("rxcui").n_unique()).collect(streaming=True).item()
        if "rxcui" in unique_after_schema and output_rows else 0
    )

    stats = {
        "clean_rows": int(clean_rows),
        "unique_drug_before": int(enriched_unique),
        "output_rows": int(output_rows),
        "mapped_rows": int(mapped_rows),
        "missing_rows": int(missing_rows),
        "unique_drug_after": int(unique_after),
        "unique_drug_missing": int(max(0, enriched_unique - unique_after)),
    }
    return stats


def run(ctx: PipelineContext) -> None:
    manifest_payload: Dict[str, Dict[str, object]] = {}
    finals: Dict[str, int] = {}

    for cohort in ("pediatric", "adult"):
        _log_box(cohort, "Finalize cohort", status="start")
        # Use Polars batched backend to avoid OOM
        stats = _finalize_cohort_polars_batched(cohort, ctx)
        out_path = output_dataset_path(ctx, cohort, "patient_report_reporter_drug_reaction_full_data.parquet")
        out_scan = pl.scan_parquet(str(out_path)) if out_path.exists() else None

        after = stats.get("output_rows", 0)
        mapped = (
            out_scan.filter(pl.col("meddra_concept_id").is_not_null())
            .select(pl.len())
            .collect(streaming=True)
            .item()
            if after and out_scan is not None
            else 0
        )
        rxnorm_mapped = (
            out_scan.filter(pl.col("rxcui").is_not_null())
            .select(pl.len())
            .collect(streaming=True)
            .item()
            if after and out_scan is not None
            else 0
        )

        dq_summary_markdown(
            ctx,
            "s09_finalize_merge_and_report",
            cohort,
            stats.get("clean_rows", 0),
            after,
            {
                "meddra_coverage_pct": f"{(mapped / after * 100) if after else 0:.2f}",
                "rxnorm_coverage_pct": f"{(rxnorm_mapped / after * 100) if after else 0:.2f}",
            },
        )

        manifest_payload[cohort] = {
            **stats,
            "meddra_mapped": mapped,
            "rxnorm_mapped": rxnorm_mapped,
            "output_path": str(out_path),
        }
        finals[cohort] = after

        _log_box(
            cohort,
            "Coverage metrics",
            rows=after,
            meddra_coverage=f"{(mapped / after * 100) if after else 0:.2f}%",
            rxnorm_coverage=f"{(rxnorm_mapped / after * 100) if after else 0:.2f}%",
            drug_unique_before=stats.get("unique_drug_before", 0),
            drug_unique_mapped=stats.get("unique_drug_after", 0),
            drug_unique_missing=stats.get("unique_drug_missing", 0),
        )

    _render_dataset_readme(ctx, finals.get("adult", 0), finals.get("pediatric", 0))

    write_manifest(
        ctx,
        "s09_finalize_merge_and_report",
        {"stage": "s09_finalize_merge_and_report", "cohorts": manifest_payload},
    )
    _log_box("summary", "Stage complete", output_root=ctx.config.paths.output_root)


def _render_dataset_readme(ctx: PipelineContext, adult_rows: int, pediatric_rows: int) -> None:
    readme_path = ctx.config.paths.output_root / "README.md"
    with readme_path.open("w", encoding="utf-8") as fh:
        fh.write("# CAN Drug Pipeline Dataset\n\n")
        fh.write(
            "This dataset contains adult and pediatric adverse drug reaction (ADR) cohorts derived from FAERS/OpenFDA.\n\n"
        )
        fh.write("## Structure\n")
        fh.write("- `Adult/*.parquet`\n")
        fh.write("- `Pediatric/*.parquet`\n")
        fh.write("- `_summary/*.md` per cohort\n\n")
        fh.write("## Key Columns\n")
        fh.write("- `safetyreportid`: OpenFDA safety report identifier\n")
        fh.write("- `age_years`: Patient age in years\n")
        fh.write("- `medicinal_product`: Reported drug name (FAERS)\n")
        fh.write("- `reaction_meddrapt`: Reported reaction term\n")
        fh.write("- `meddra_concept_*`: Linked OMOP/MedDRA identifiers\n")
        fh.write("- `rxcui`: RxNorm concept identifier\n")
        fh.write("- `rxcui`: RxNorm concept — **use this to count distinct drugs** (ROR/PRR, rankings)\n")
        fh.write("- `ingredient`: List[str] — RxNorm ingredients for this rxcui\n")
        fh.write("- `faers_ingredients`: List[str] — structured ingredients from FAERS/S07b\n")
        fh.write("- `ingredient_count`, `faers_ingredient_count`, `is_combination_product`\n\n")
        fh.write("## Summary\n")
        fh.write(f"- Adult rows: {adult_rows:,}\n")
        fh.write(f"- Pediatric rows: {pediatric_rows:,}\n")
        fh.write(f"- Generated by run `{ctx.run_id}` on {ctx.run_ts.isoformat()}\n")


__all__ = ["run"]
