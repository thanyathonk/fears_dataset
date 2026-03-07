from __future__ import annotations

"""Stage S07b – Validate LLM-cleaned drug names."""

from pathlib import Path
from typing import Dict

import polars as pl
from loguru import logger
from tqdm import tqdm

from src.utils.dq import dq_summary_markdown
from src.utils.io import PipelineContext, stage_output_path, write_manifest


def run(ctx: PipelineContext) -> None:
    """
    Stage S07b: Validate and prepare LLM-cleaned drug names for RxNorm lookup.
    
    IMPORTANT: This stage expects LLM cleaning to be done externally (e.g., on Slurm).
    
    Workflow:
    1. S07 outputs: s07_split_drug/{cohort}_drugs_full_data.parquet
       Columns: index, medicinal_product
       
    2. User runs LLM cleaning script on Slurm cluster
       Input: {cohort}_drugs_full_data.parquet
       Output: {cohort}_drugs_llm_cleaned_full_data.parquet
       
    3. LLM script must add column: 'medicinal_product_llm_clean'
       
    4. This stage (S07b) validates the output and prepares for S08
    
    Expected input file location:
        s07_split_drug/{cohort}_drugs_llm_cleaned_full_data.parquet
        
    Expected columns:
        - index (from S07)
        - medicinal_product (from S07)
        - medicinal_product_llm_clean (from LLM script)
    """
    input_dir = stage_output_path(ctx, "s07_split_drug")
    output_dir = stage_output_path(ctx, "s07b_llm_clean")
    output_dir.mkdir(parents=True, exist_ok=True)

    manifest_payload: Dict[str, Dict[str, object]] = {}

    for cohort in tqdm(("pediatric", "adult"), desc="S07b cohorts"):
        logger.info(f"[S07b] Validating LLM-cleaned drugs for {cohort}")
        
        # LLM-cleaned file (should be created by external script)
        llm_cleaned_path = input_dir / f"{cohort}_drugs_llm_cleaned_full_data.parquet"
        
        if not llm_cleaned_path.exists():
            error_msg = (
                f"\n{'='*80}\n"
                f"[S07b] ERROR: LLM-cleaned file not found!\n"
                f"{'='*80}\n"
                f"Expected file: {llm_cleaned_path}\n"
                f"\n"
                f"Please complete these steps:\n"
                f"1. Copy input file to Slurm cluster:\n"
                f"   {input_dir / f'{cohort}_drugs_full_data.parquet'}\n"
                f"\n"
                f"2. Run LLM cleaning script (see scripts/llm_clean_drugs.py)\n"
                f"\n"
                f"3. Copy output back:\n"
                f"   Output should have columns: index, medicinal_product, medicinal_product_llm_clean\n"
                f"\n"
                f"4. Save as: {llm_cleaned_path}\n"
                f"{'='*80}\n"
            )
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)

        # Load and validate
        df = pl.read_parquet(llm_cleaned_path)
        
        # Check required columns
        required_cols = ["index", "medicinal_product", "medicinal_product_llm_clean"]
        missing_cols = [c for c in required_cols if c not in df.columns]
        if missing_cols:
            raise ValueError(
                f"[S07b] Missing required columns: {missing_cols}\n"
                f"       Found columns: {df.columns}\n"
                f"       LLM script must output: {required_cols}"
            )

        # Filter valid cleaned names
        df_clean = df.filter(
            pl.col("medicinal_product_llm_clean").is_not_null() &
            (pl.col("medicinal_product_llm_clean").str.len_chars() > 0)
        )

        # Save validated output
        out_path = output_dir / f"{cohort}_drugs_clean_full_data.parquet"
        df_clean.write_parquet(out_path, compression="zstd")

        # Calculate stats
        coverage_pct = df_clean.height / df.height * 100 if df.height > 0 else 0
        
        dq_summary_markdown(
            ctx,
            "s07b_llm_clean",
            cohort,
            df.height,
            df_clean.height,
            {"llm_coverage_pct": f"{coverage_pct:.2f}"},
        )

        logger.info(
            f"[S07b] {cohort}: {df_clean.height:,}/{df.height:,} drugs cleaned successfully "
            f"({coverage_pct:.1f}% coverage)"
        )

        manifest_payload[cohort] = {
            "input_drugs": df.height,
            "cleaned_drugs": df_clean.height,
            "coverage_pct": coverage_pct,
            "output_path": str(out_path),
        }

    write_manifest(
        ctx,
        "s07b_llm_clean",
        {"stage": "s07b_llm_clean", "cohorts": manifest_payload},
    )
    logger.success("[S07b] LLM cleaning validation complete")


__all__ = ["run"]

