from __future__ import annotations

"""Stage S05 – Map ADRs to MedDRA hierarchies using Polars.

Updated to use CONCEPT_ANCESTOR for PT->SOC mapping (instead of full hierarchy via CONCEPT_RELATIONSHIP).
Text matching uses Title Case instead of UPPERCASE for better compatibility with MedDRA standards.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Dict

import polars as pl
from loguru import logger
from tqdm import tqdm

from src.utils.dq import dq_summary_markdown
from src.utils.io import PipelineContext, stage_output_path, write_manifest
from src.utils.meddra import (
    EXCLUDED_SOCS,
    VOCAB_DIRNAME,
    concept_subset as _concept_subset,
    get_vocab_version_info,
    load_concepts as _load_concepts,
    vocab_dir as _vocab_dir_shared,
)

# ============================================================================
# REACTION TEXT FILTERING REMOVED
# ============================================================================
# NOTE: Filter reaction patterns (exclude inefficacy, disease progression, errors) 
# has been removed - all reaction terms are now kept


@dataclass(slots=True)
class MedDRAResources:
    """MedDRA vocabulary resources for ADR mapping."""
    pt: pl.DataFrame
    soc: pl.DataFrame
    concept_ancestor: pl.DataFrame


def _vocab_dir(ctx: PipelineContext) -> Path:
    return _vocab_dir_shared(ctx, stage_tag="S06")


def _load_concept_ancestor(ancestor_path: Path, pt_ids: list, soc_ids: list) -> pl.DataFrame:
    """Load CONCEPT_ANCESTOR table filtered for MedDRA PT->SOC relationships only.
    
    CONCEPT_ANCESTOR has 80M+ rows covering all vocabularies (SNOMED, RxNorm, ATC, MedDRA).
    We only need MedDRA PT->SOC relationships to avoid OOM.
    """
    logger.info(f"[S05] Loading CONCEPT_ANCESTOR (filtering for {len(pt_ids)} PTs -> {len(soc_ids)} SOCs)...")
    
    return (
        pl.read_csv(
            ancestor_path,
            separator="\t",
            has_header=True,
            schema_overrides={
                "ancestor_concept_id": pl.Int64,
                "descendant_concept_id": pl.Int64,
            },
            quote_char=None,
            null_values="",
        )
        .select(
            pl.col("ancestor_concept_id"),
            pl.col("descendant_concept_id"),
        )
        # Filter for MedDRA PT->SOC relationships only
        .filter(
            pl.col("descendant_concept_id").is_in(pt_ids) &
            pl.col("ancestor_concept_id").is_in(soc_ids)
        )
    )


def _build_meddra_resources(concept_dir: Path) -> MedDRAResources:
    """Build MedDRA resources using CONCEPT_ANCESTOR for PT->SOC mapping.
    
    This approach uses the CONCEPT_ANCESTOR table to find direct ancestor-descendant
    relationships between PT (Preferred Terms) and SOC (System Organ Class),
    instead of building the full HLT->HLGT->SOC hierarchy.
    
    Memory optimization: Filter CONCEPT_ANCESTOR to only MedDRA PT->SOC relationships
    before loading to avoid OOM (80M+ rows → ~50K rows).
    """
    concept_path = concept_dir / "CONCEPT.csv"
    ancestor_path = concept_dir / "CONCEPT_ANCESTOR.csv"

    logger.info("[S05] Loading CONCEPT.csv...")
    concept_df = _load_concepts(concept_path)

    pt_df = _concept_subset(concept_df, "PT", "pt")
    soc_df = _concept_subset(concept_df, "SOC", "soc")

    # Get IDs for filtering CONCEPT_ANCESTOR
    pt_ids = pt_df["pt_id"].to_list()
    soc_ids = soc_df["soc_id"].to_list()

    # Load CONCEPT_ANCESTOR with filtering
    concept_ancestor = _load_concept_ancestor(ancestor_path, pt_ids, soc_ids)

    logger.info(
        f"[S05] Loaded {pt_df.height} PT concepts, {soc_df.height} SOC concepts, "
        f"{concept_ancestor.height:,} PT->SOC relationships"
    )

    return MedDRAResources(
        pt=pt_df,
        soc=soc_df,
        concept_ancestor=concept_ancestor,
    )


def _map_cohort_adr(
    adr_path: Path,
    resources: MedDRAResources,
    export_dir: Path,
    cohort: str,
) -> Dict[str, int]:
    """Map ADR terms to MedDRA PT and SOC using CONCEPT_ANCESTOR.
    
    Key changes from previous version:
    - Uses Title Case instead of UPPERCASE for term matching
    - Uses CONCEPT_ANCESTOR for PT->SOC mapping (not full hierarchy)
    - Supports multiple SOC per PT (grouped as lists)
    """
    # Load and normalize ADR data with Title Case
    adr_lf = (
        pl.scan_parquet(adr_path)
        .select(
            pl.col("safetyreportid").cast(pl.Utf8, strict=False),
            pl.col("reaction_meddrapt").cast(pl.Utf8, strict=False),
            pl.col("reaction_outcome").cast(pl.Utf8, strict=False),
        )
        .with_columns(
            pl.col("reaction_meddrapt")
            .fill_null("")
            .str.strip_chars()
            .alias("reaction_meddrapt"),
            pl.col("reaction_meddrapt")
            .str.to_titlecase()
            .alias("reaction_term"),
            pl.col("reaction_outcome")
            .fill_null("")
            .str.strip_chars()
            .alias("reaction_outcome"),
        )
    )

    pt_lazy = resources.pt.lazy()

    # Step 1: Map ADR terms to PT using Title Case matching
    logger.info(f"[S05][{cohort.upper()}] Step 1/3: Mapping ADR terms to MedDRA PT (Title Case)...")
    mapped_pt = (
        adr_lf
        .join(
            pt_lazy,
            left_on="reaction_term",
            right_on="pt_name_match",
            how="left",
        )
        .collect(streaming=True)
        .unique(subset=["safetyreportid", "reaction_meddrapt", "reaction_outcome", "pt_id"])
    )

    # Step 2: Use CONCEPT_ANCESTOR to find PT->SOC relationships
    logger.info(f"[S05][{cohort.upper()}] Step 2/3: Mapping PT to SOC using CONCEPT_ANCESTOR...")
    
    # Join with CONCEPT_ANCESTOR to get SOC ancestors
    mapped_with_ancestor = (
        mapped_pt
        .join(
            resources.concept_ancestor,
            left_on="pt_id",
            right_on="descendant_concept_id",
            how="left"
        )
    )
    
    # Join with SOC concepts to get SOC details
    mapped_with_soc = (
        mapped_with_ancestor
        .join(
            resources.soc,
            left_on="ancestor_concept_id",
            right_on="soc_id",
            how="left"
        )
        .filter(pl.col("ancestor_concept_id").is_not_null())  # Keep only rows that mapped to SOC
    )
    
    # Rename columns to match expected output format
    mapped_full = (
        mapped_with_soc
        .rename(
            {
                "pt_id": "meddra_concept_id",
                "pt_code": "meddra_concept_code",
                "pt_name": "meddra_concept_name",
                "pt_class": "meddra_concept_class_id",
                "ancestor_concept_id": "meddra_soc_id",  # Use ancestor_concept_id as SOC ID
                "soc_code": "meddra_soc_code",
                "soc_name": "meddra_soc_name",
                "soc_class": "meddra_soc_class_id",
            }
        )
        .drop(
            [
                "pt_name_match",
                "soc_name_match",
                "reaction_term",
                "descendant_concept_id",
            ],
            strict=False,
        )
        .with_columns(
            pl.col("meddra_concept_id").cast(pl.Int64),
            pl.col("meddra_concept_code").cast(pl.Utf8, strict=False),
            pl.col("meddra_soc_id").cast(pl.Int64),
            pl.col("meddra_soc_code").cast(pl.Utf8, strict=False),
    )
    )

    # Create PT->SOC dictionary (Title Case for matching)
    # This is the ONLY output needed for Stage 9 to merge with full dataset
    cohort_dir = export_dir / cohort
    cohort_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"[S05][{cohort.upper()}] Step 3/3: Filtering excluded SOC categories...")
    
    # Filter out excluded SOC categories only (reaction pattern filtering removed)
    mapped_filtered = mapped_full.filter(
        pl.col("meddra_concept_id").is_not_null() &
        ~pl.col("meddra_soc_name").is_in(EXCLUDED_SOCS)
    )
    
    soc_filtered_count = mapped_full.height - mapped_filtered.height
    logger.info(f"[S05][{cohort.upper()}]   → Removed {soc_filtered_count:,} rows with excluded SOCs")
    
    # Group by PT and aggregate SOCs into lists (1 PT can have multiple SOCs)
    # This matches the notebook logic: groupby().agg(list(set(x)))
    pt_soc_dict = (
        mapped_filtered
        .group_by([
            "meddra_concept_name",   # Title Case (for matching)
            "meddra_concept_id",     # Need for output
            "meddra_concept_code",   # Need for output
        ])
        .agg([
            pl.col("meddra_soc_name").unique().alias("meddra_soc_names"),   # List of unique SOC names
            pl.col("meddra_soc_code").unique().alias("meddra_soc_codes"),   # List of unique SOC codes
        ])
        .sort("meddra_concept_name")
        .rename({
            "meddra_concept_name": "MedDRA_concept_name",
        })
    )

    # Write PT-SOC dictionary (used by Stage 9)
    dict_parquet_path = cohort_dir / "pt_soc_dictionary_full_data.parquet"
    pt_soc_dict.write_parquet(dict_parquet_path, compression="zstd")
    
    logger.info(
        f"[S05][{cohort.upper()}] Created dictionary: {pt_soc_dict.height:,} unique PTs "
        f"(excluded {len(EXCLUDED_SOCS)} SOC categories: {', '.join(EXCLUDED_SOCS)})"
    )

    # Calculate statistics
    adr_total = (
        pl.scan_parquet(adr_path)
        .select(pl.len())
        .collect(streaming=True)
        .item()
    )
    mapped = mapped_pt.filter(pl.col("pt_id").is_not_null()).height
    coverage = 0.0 if adr_total == 0 else mapped / adr_total * 100
    
    # Count PTs before and after SOC filtering
    unique_pts = pt_soc_dict.height
    # Count unique SOCs from the list column
    unique_socs = pt_soc_dict["meddra_soc_names"].explode().n_unique()
    
    logger.success(
        f"[S05][{cohort.upper()}] ✓ Completed: {mapped:,}/{adr_total:,} ADRs mapped ({coverage:.2f}%), "
        f"{unique_pts:,} PTs, {unique_socs} SOCs"
    )

    return {
        "adr_rows": int(adr_total),
        "mapped_rows": int(mapped),
        "coverage_pct": coverage,
        "unique_pts": int(unique_pts),
        "unique_socs": int(unique_socs),
    }


def run(ctx: PipelineContext) -> None:
    vocab_dir = _vocab_dir(ctx)
    resources = _build_meddra_resources(vocab_dir)

    input_dir = stage_output_path(ctx, "s04_split_adr")
    output_dir = stage_output_path(ctx, "s05_map_omop_meddra")
    output_dir.mkdir(parents=True, exist_ok=True)

    manifest_payload: Dict[str, Dict[str, object]] = {}

    logger.info("=" * 80)
    logger.info(f"[S05] Starting MedDRA mapping for 2 cohorts (adult, pediatric)")
    logger.info("=" * 80)

    for cohort in ("pediatric", "adult"):
        logger.info("")
        logger.info("┌" + "─" * 78 + "┐")
        logger.info(f"│ Processing Cohort: {cohort.upper():<63} │")
        logger.info("└" + "─" * 78 + "┘")
        
        adr_path = input_dir / f"{cohort}_adr_full_data.parquet"
        if not adr_path.exists():
            raise FileNotFoundError(f"Missing ADR dataset for cohort {cohort}")

        stats = _map_cohort_adr(
            adr_path=adr_path,
            resources=resources,
            export_dir=output_dir,
            cohort=cohort,
        )

        dq_summary_markdown(
            ctx,
            "s05_map_omop_meddra",
            cohort,
            stats["adr_rows"],
            stats["mapped_rows"],
            {"meddra_coverage_pct": f"{stats['coverage_pct']:.2f}"},
        )

        dict_path = output_dir / cohort / "pt_soc_dictionary_full_data.parquet"
        manifest_payload[cohort] = {
            "input_rows": stats["adr_rows"],
            "mapped_rows": stats["mapped_rows"],
            "coverage_pct": stats["coverage_pct"],
            "unique_pts": stats["unique_pts"],
            "unique_socs": stats["unique_socs"],
            "dictionary_path": str(dict_path),
        }

    # Summary table
    logger.info("")
    logger.info("=" * 80)
    logger.info("[S05] MedDRA Mapping Summary")
    logger.info("=" * 80)
    logger.info("")
    logger.info("┌────────────┬──────────────┬──────────────┬────────────┬─────────────┬──────────┐")
    logger.info("│ Cohort     │ Input ADRs   │ Mapped ADRs  │ Coverage   │ Unique PTs  │ Unique   │")
    logger.info("│            │              │              │            │             │ SOCs     │")
    logger.info("├────────────┼──────────────┼──────────────┼────────────┼─────────────┼──────────┤")
    
    for cohort in ("pediatric", "adult"):
        stats = manifest_payload[cohort]
        logger.info(
            f"│ {cohort:<10} │ {stats['input_rows']:>12,} │ {stats['mapped_rows']:>12,} │ "
            f"{stats['coverage_pct']:>9.2f}% │ {stats['unique_pts']:>11,} │ "
            f"{stats['unique_socs']:>8} │"
        )
    
    logger.info("└────────────┴──────────────┴──────────────┴────────────┴─────────────┴──────────┘")
    logger.info("")
    
    # Additional info
    logger.info("Output files:")
    for cohort in ("pediatric", "adult"):
        dict_path = manifest_payload[cohort]["dictionary_path"]
        logger.info(f"  • {cohort}: {dict_path}")
    
    logger.info("")
    logger.info("Notes:")
    logger.info(f"  • Text matching: Title Case normalization")
    logger.info(f"  • Excluded SOCs: {len(EXCLUDED_SOCS)} categories")
    logger.info(f"    ({', '.join(EXCLUDED_SOCS)})")
    logger.info(f"  • Reaction pattern filtering: REMOVED (all reaction terms kept)")
    logger.info("")

    vocab_version = get_vocab_version_info(vocab_dir)
    logger.info(f"[S05] Vocabulary version info: {vocab_version}")

    write_manifest(
        ctx,
        "s05_map_omop_meddra",
        {
            "stage": "s05_map_omop_meddra",
            "vocabulary": vocab_version,
            "cohorts": manifest_payload,
        },
    )
    logger.success("[S05] MedDRA mapping complete")


__all__ = ["run"]
