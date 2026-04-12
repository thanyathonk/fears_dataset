"""Shared MedDRA vocabulary helpers used by S06 and S06b.

Centralises vocabulary directory resolution, concept loading, and subset
extraction so that both stages (CONCEPT_ANCESTOR shortcut and full hierarchy)
share exactly the same data-loading logic.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
from loguru import logger

from src.utils.io import PipelineContext

# Directory name under vocab_root that contains OMOP vocabulary CSV files
VOCAB_DIRNAME = "vocabulary_SNOMED_MEDDRA_RxNorm_ATC"

# SOC categories unrelated to direct drug effects — excluded from PT-SOC dictionaries
# - Surgical/medical procedures: interventions, not reactions
# - Social circumstances: non-medical factors
# - Product issues: manufacturing/quality problems, not patient reactions
EXCLUDED_SOCS = [
    "Surgical and medical procedures",
    "Social circumstances",
    "Product issues",
]


def vocab_dir(ctx: PipelineContext, *, stage_tag: str = "S06") -> Path:
    """Resolve the MedDRA vocabulary directory from config, with legacy fallback.

    Raises FileNotFoundError if neither candidate path exists.
    """
    candidate = ctx.config.paths.vocab_root / VOCAB_DIRNAME
    if candidate.exists():
        return candidate
    legacy = ctx.config.paths.root / "vocab" / VOCAB_DIRNAME
    if legacy.exists():
        logger.warning(
            "[%s] Found vocabulary in legacy location %s; please migrate to %s",
            stage_tag,
            legacy,
            candidate,
        )
        return legacy
    raise FileNotFoundError(
        "MedDRA vocabulary directory not found. Expected at "
        f"{candidate}. Please ensure vocab files are available."
    )


def load_concepts(concept_path: Path) -> pl.DataFrame:
    """Load MedDRA concepts from CONCEPT.csv with Title Case normalisation.

    Returns a DataFrame with columns:
      concept_id (Int64), concept_code (Utf8), concept_name (Utf8),
      concept_class_id (Utf8), concept_name_title (Utf8).
    """
    return (
        pl.read_csv(
            concept_path,
            separator="\t",
            infer_schema_length=0,
            quote_char=None,
            null_values="",
        )
        .filter(pl.col("vocabulary_id") == "MedDRA")
        .select(
            pl.col("concept_id").cast(pl.Int64),
            pl.col("concept_code").cast(pl.Utf8, strict=False),
            pl.col("concept_name").cast(pl.Utf8, strict=False),
            pl.col("concept_class_id").cast(pl.Utf8, strict=False),
        )
        .with_columns(
            pl.col("concept_name")
            .fill_null("")
            .str.to_titlecase()
            .alias("concept_name_title")
        )
    )


def concept_subset(df: pl.DataFrame, cls: str, prefix: str) -> pl.DataFrame:
    """Extract a concept subset by class_id and rename columns with *prefix*.

    Example: ``concept_subset(df, "PT", "pt")`` returns columns
    ``pt_id``, ``pt_code``, ``pt_name``, ``pt_name_match``, ``pt_class``.
    """
    return (
        df.filter(pl.col("concept_class_id") == cls)
        .select(
            pl.col("concept_id").alias(f"{prefix}_id"),
            pl.col("concept_code").alias(f"{prefix}_code"),
            pl.col("concept_name").alias(f"{prefix}_name"),
            pl.col("concept_name_title").alias(f"{prefix}_name_match"),
            pl.col("concept_class_id").alias(f"{prefix}_class"),
        )
    )
