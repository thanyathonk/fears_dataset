from __future__ import annotations

"""Stage S05 – Split ADR tables."""

from pathlib import Path
from typing import Dict

import polars as pl
from loguru import logger
from tqdm import tqdm

from src.utils.dq import dq_summary_markdown
from src.utils.io import PipelineContext, stage_output_path, write_manifest


def _extract_adr_lazy(path: Path) -> pl.LazyFrame:
    return (
        pl.scan_parquet(path)
        .select(
            pl.col("safetyreportid").cast(pl.Utf8, strict=False),
            pl.col("reaction_meddrapt").cast(pl.Utf8, strict=False),
            pl.col("reaction_outcome").cast(pl.Utf8, strict=False),
        )
        .drop_nulls(["reaction_meddrapt"])
        .unique()
        .with_columns(
            pl.col("reaction_meddrapt").str.strip_chars(),
            pl.col("reaction_outcome").str.strip_chars(),
        )
    )


def run(ctx: PipelineContext) -> None:
    # ─────────────────────────────────────────────────────────────────────────
    # S05 now reads directly from S03 (skip S04 - cleaning moved to S03)
    # ─────────────────────────────────────────────────────────────────────────
    source_dir = stage_output_path(ctx, "s03_join_partition_age")
    output_dir = stage_output_path(ctx, "s05_split_adr")
    output_dir.mkdir(parents=True, exist_ok=True)

    manifest_payload: Dict[str, Dict[str, object]] = {}
    for cohort in tqdm(("pediatric", "adult"), desc="S05 cohorts"):
        # S03 output format: {cohort}_events_full_data.parquet
        source = source_dir / f"{cohort}_events_full_data.parquet"
        if not source.exists():
            raise FileNotFoundError(f"Missing events dataset for cohort {cohort} at {source}")

        adr_lf = _extract_adr_lazy(source)
        out_path = output_dir / f"{cohort}_adr_full_data.parquet"
        adr_lf.sink_parquet(out_path, compression="zstd", statistics=True)

        input_rows = (
            pl.scan_parquet(source)
            .select(pl.len())
            .collect(streaming=True)
            .item()
        )
        adr_stats = (
            pl.scan_parquet(out_path)
            .select(
                pl.len().alias("rows"),
                pl.col("reaction_meddrapt").n_unique().alias("unique_terms"),
            )
            .collect(streaming=True)
        )

        adr_rows = int(adr_stats["rows"][0])
        unique_terms = int(adr_stats["unique_terms"][0])

        dq_summary_markdown(
            ctx,
            "s05_split_adr",
            cohort,
            input_rows,
            adr_rows,
            {"unique_terms": unique_terms},
        )

        manifest_payload[cohort] = {
            "input_rows": input_rows,
            "adr_rows": adr_rows,
            "unique_terms": unique_terms,
            "output_path": str(out_path),
        }

        logger.info(
            f"[S05] Cohort {cohort} ADR rows={adr_rows:,} unique_terms={unique_terms:,}"
        )

    write_manifest(ctx, "s05_split_adr", {"stage": "s05_split_adr", "cohorts": manifest_payload})
    logger.success("[S05] ADR extraction complete")
