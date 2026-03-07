from __future__ import annotations

"""Data quality utilities leveraging Pandera schemas."""

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

import pandera as pa
import polars as pl

from src.utils.io import PipelineContext, write_manifest


@dataclass
class DQResult:
    name: str
    passed: bool
    errors: Optional[str]
    checked_rows: int


def validate_schema(
    ctx: PipelineContext,
    stage: str,
    name: str,
    df: pl.DataFrame,
    schema: pa.DataFrameSchema,
) -> DQResult:
    try:
        schema.validate(df.to_pandas(), lazy=True)
        result = DQResult(name=name, passed=True, errors=None, checked_rows=df.height)
    except pa.errors.SchemaError as exc:  # pragma: no cover - exercise specific path
        result = DQResult(name=name, passed=False, errors=str(exc), checked_rows=df.height)
    payload: Dict[str, object] = {
        "dq_name": name,
        "passed": result.passed,
        "rows": result.checked_rows,
        "errors": result.errors,
    }
    write_manifest(ctx, f"{stage}_{name}_dq", payload)
    return result


def _cohort_dir_name(cohort: str) -> str:
    mapping = {"adult": "Adult", "pediatric": "Pediatric"}
    return mapping.get(cohort.lower(), cohort)


def dq_summary_markdown(
    ctx: PipelineContext,
    stage: str,
    cohort: str,
    before: int,
    after: int,
    extras: Optional[Dict[str, object]] = None,
) -> Path:
    cohort_name = _cohort_dir_name(cohort)
    cohort_dir = ctx.config.paths.output_root / cohort_name
    summary_dir = cohort_dir / "_summary"
    summary_dir.mkdir(parents=True, exist_ok=True)
    path = summary_dir / f"{stage}.md"
    delta = after - before
    pct = 0 if before == 0 else (delta / before) * 100
    with path.open("w", encoding="utf-8") as fh:
        fh.write(f"# {stage} Summary (cohort: {cohort_name})\n\n")
        fh.write(f"- Rows before: {before:,}\n")
        fh.write(f"- Rows after: {after:,}\n")
        fh.write(f"- Delta: {delta:+,} ({pct:+.2f}%)\n")
        if extras:
            fh.write("\n## Metrics\n")
            for key, value in extras.items():
                fh.write(f"- {key}: {value}\n")
    return path


__all__ = ["DQResult", "validate_schema", "dq_summary_markdown"]
