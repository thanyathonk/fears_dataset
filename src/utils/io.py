from __future__ import annotations

"""IO helpers and run context utilities for the can-drug pipeline."""

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import polars as pl
from loguru import logger

from src.settings import PipelineConfig


@dataclass(slots=True)
class PipelineContext:
    """Runtime context shared across stages."""

    config: PipelineConfig
    run_id: str
    run_ts: datetime
    run_dir: Path
    log_path: Path

    @property
    def cache_dir(self) -> Path:
        return self.config.paths.cache_root


def _timestamp_run_id() -> str:
    return datetime.utcnow().strftime("%Y%m%dT%H%M%S")


def init_run_context(config: PipelineConfig, run_id: Optional[str] = None) -> PipelineContext:
    run_identifier = run_id or _timestamp_run_id()
    run_ts = datetime.utcnow()
    run_dir = config.paths.logs_root / run_identifier
    run_dir.mkdir(parents=True, exist_ok=True)

    log_path = run_dir / "pipeline.log"
    logger.remove()
    logger.add(log_path, enqueue=True, backtrace=True, diagnose=False)
    logger.add(lambda msg: print(msg, end=""))

    for path in (
        config.paths.data_root,
        config.paths.staging_root,
        config.paths.vocab_root,
        config.paths.output_root,
        config.paths.output_root / "Adult",
        config.paths.output_root / "Pediatric",
        config.paths.cache_root,
        config.paths.logs_root,
    ):
        path.mkdir(parents=True, exist_ok=True)

    return PipelineContext(
        config=config,
        run_id=run_identifier,
        run_ts=run_ts,
        run_dir=run_dir,
        log_path=log_path,
    )


def write_manifest(ctx: PipelineContext, stage: str, payload: Dict[str, Any]) -> Path:
    manifest_dir = ctx.run_dir / "manifests"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = manifest_dir / f"{stage}.json"
    with manifest_path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2, default=str)
    return manifest_path


def load_parquet(path: Path | str) -> pl.DataFrame:
    return pl.read_parquet(path)


def write_parquet(df: pl.DataFrame, path: Path | str, *, compression: str = "zstd", statistics: bool = True) -> Path:
    out_path = Path(path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out_path, compression=compression, statistics=statistics)
    return out_path


def stage_output_path(ctx: PipelineContext, stage: str, cohort: Optional[str] = None, filename: str = "") -> Path:
    base = ctx.config.paths.staging_root / stage
    if cohort:
        base = base / cohort
    base.mkdir(parents=True, exist_ok=True)
    return base / filename if filename else base


def output_dataset_path(ctx: PipelineContext, cohort: str, filename: str) -> Path:
    base = ctx.config.paths.output_root / cohort.capitalize()
    base.mkdir(parents=True, exist_ok=True)
    return base / filename


__all__ = [
    "PipelineContext",
    "init_run_context",
    "write_manifest",
    "load_parquet",
    "write_parquet",
    "stage_output_path",
    "output_dataset_path",
]
