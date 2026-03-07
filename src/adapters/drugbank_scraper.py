from __future__ import annotations

"""Thin wrapper around legacy DrugBank scraper (optional stage)."""

from pathlib import Path
from typing import Optional

from loguru import logger

from src.utils.io import PipelineContext


def run_drugbank_scraper(ctx: PipelineContext, input_path: Path, output_dir: Optional[Path] = None) -> None:
    logger.warning(
        "DrugBank scraping is optional and requires manual review; keeping legacy script in step_script/production_drugbank_scraper.py."
    )
    logger.info("If enabled, invoke the external script with appropriate parameters.")


__all__ = ["run_drugbank_scraper"]
