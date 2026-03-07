from __future__ import annotations

"""Stage S07 – Extract unique drug names for LLM cleaning."""

from pathlib import Path
from typing import Dict

import polars as pl
from loguru import logger
from tqdm import tqdm

from src.utils.dq import dq_summary_markdown
from src.utils.io import PipelineContext, stage_output_path, write_manifest


def _extract_unique_drugs(path: Path) -> pl.DataFrame:
    """
    Extract unique drug names with index.
    
    Output columns: index, medicinal_product
    """
    return (
        pl.scan_parquet(path)
        .select(
            pl.col("medicinal_product").cast(pl.Utf8, strict=False)
        )
        .drop_nulls("medicinal_product")
        .filter(pl.col("medicinal_product").str.len_chars() > 0)
        .unique()
        .collect(streaming=True)
        .with_row_count("index")  # Add index column
    )


def run(ctx: PipelineContext) -> None:
    # Updated to read from S03 after early filtering (instead of S04)
    source_dir = stage_output_path(ctx, "s03_join_partition_age")
    output_dir = stage_output_path(ctx, "s07_split_drug")
    output_dir.mkdir(parents=True, exist_ok=True)

    manifest_payload: Dict[str, Dict[str, object]] = {}

    for cohort in tqdm(("pediatric", "adult"), desc="S07 cohorts"):
        logger.info(f"[S07] Extracting unique drugs for {cohort}")
        
        source = source_dir / f"{cohort}_events_full_data.parquet"
        if not source.exists():
            raise FileNotFoundError(f"Missing cleaned dataset: {source}")

        # Extract unique drugs
        unique_drugs = _extract_unique_drugs(source)

        # Save for LLM cleaning
        out_path = output_dir / f"{cohort}_drugs_full_data.parquet"
        unique_drugs.write_parquet(out_path, compression="zstd")

        # Stats
        input_rows = (
            pl.scan_parquet(source)
            .select(pl.len())
            .collect(streaming=True)
            .item()
        )

        dq_summary_markdown(
            ctx,
            "s07_split_drug",
            cohort,
            input_rows,
            unique_drugs.height,
            {"unique_drugs": unique_drugs.height},
        )

        logger.info(
            f"[S07] {cohort}: {unique_drugs.height:,} unique drugs extracted "
            f"from {input_rows:,} rows"
        )

        manifest_payload[cohort] = {
            "input_rows": input_rows,
            "unique_drugs": unique_drugs.height,
            "output_path": str(out_path),
        }

    write_manifest(
        ctx,
        "s07_split_drug",
        {"stage": "s07_split_drug", "cohorts": manifest_payload},
    )
    
    logger.success("[S07] Drug extraction complete")
    logger.info(
        "[S07] Next step: Run LLM cleaning script to add 'medicinal_product_llm_clean' column"
    )


__all__ = ["run"]
