"""Utility functions for displaying stage output summaries."""

from __future__ import annotations

from pathlib import Path
from typing import List, Optional

import polars as pl
from loguru import logger


def log_output_summary(
    file_path: Path,
    stage_name: str,
    cohort: Optional[str] = None,
    key_columns: Optional[List[str]] = None,
) -> None:

    if not file_path.exists():
        logger.warning(f"[{stage_name}] Output file not found: {file_path}")
        return
    
    try:
        # Get file size
        file_size_mb = file_path.stat().st_size / (1024 * 1024)
        
        # Read schema and get column names
        schema = pl.scan_parquet(str(file_path)).collect_schema()
        columns = list(schema.keys())
        
        # Get row count
        try:
            row_count = (
                pl.scan_parquet(str(file_path))
                .select(pl.len())
                .collect(streaming=True)
                .item()
            )
        except Exception:
            import pyarrow.parquet as pq
            pf = pq.ParquetFile(str(file_path))
            row_count = pf.metadata.num_rows
        
        # Build log message
        cohort_prefix = f"[{cohort}] " if cohort else ""
        logger.info(f"[{stage_name}] {cohort_prefix}Output file: {file_path.name}")
        logger.info(f"[{stage_name}] {cohort_prefix}File size: {file_size_mb:.2f} MB")
        logger.info(f"[{stage_name}] {cohort_prefix}Total rows: {row_count:,}")
        logger.info(f"[{stage_name}] {cohort_prefix}Total columns: {len(columns)}")
        logger.info(f"[{stage_name}] {cohort_prefix}Columns: {', '.join(columns)}")
        
        # Count unique values for key columns
        if key_columns:
            key_stats = {}
            for key_col in key_columns:
                if key_col in columns:
                    try:
                        unique_count = (
                            pl.scan_parquet(str(file_path))
                            .select(pl.col(key_col).n_unique())
                            .collect(streaming=True)
                            .item()
                        )
                        key_stats[key_col] = unique_count
                    except Exception as e:
                        logger.warning(f"[{stage_name}] Could not count unique {key_col}: {e}")
                        key_stats[key_col] = None
            
            if key_stats:
                stats_str = ", ".join(
                    f"unique_{k}={v:,}" if v is not None else f"unique_{k}=N/A"
                    for k, v in key_stats.items()
                )
                logger.info(f"[{stage_name}] {cohort_prefix}Key statistics: {stats_str}")
        
        logger.info("")  # Empty line for readability
        
    except Exception as e:
        logger.error(f"[{stage_name}] Error reading output summary: {e}")


def log_output_summary_multiple(
    file_paths: List[Path],
    stage_name: str,
    key_columns: Optional[List[str]] = None,
) -> None:

    for file_path in file_paths:
        # Try to extract cohort name from filename
        cohort = None
        if "adult" in file_path.name.lower():
            cohort = "adult"
        elif "pediatric" in file_path.name.lower():
            cohort = "pediatric"
        
        log_output_summary(file_path, stage_name, cohort=cohort, key_columns=key_columns)

