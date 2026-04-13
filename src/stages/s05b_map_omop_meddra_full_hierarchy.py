from __future__ import annotations

"""Stage S05b – Map ADRs to MedDRA full hierarchy (PT→HLT→HLGT→SOC).

Uses CONCEPT_RELATIONSHIP to build the complete MedDRA hierarchy instead of
the shortcut CONCEPT_ANCESTOR used in S06.  All reaction terms are kept
(no exclude/rescue pattern filtering) to preserve the full dataset.

Outputs per cohort:
1. pt_hierarchy_dictionary_full_data.parquet  – PT-centric dict with aggregated
   lists for HLT, HLGT, and SOC levels.
2. pt_hierarchy_paths_full_data.parquet       – Expanded rows: one row per
   unique PT→HLT→HLGT→SOC path (useful for granular analysis).
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Dict

import polars as pl
from loguru import logger

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


# ---------------------------------------------------------------------------
# Data class
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class MedDRAResources:
    """MedDRA vocabulary resources for full-hierarchy ADR mapping."""
    pt: pl.DataFrame
    hlt: pl.DataFrame
    hlgt: pl.DataFrame
    soc: pl.DataFrame
    pt_to_hlt: pl.DataFrame    # Edge table: PT → HLT
    hlt_to_hlgt: pl.DataFrame  # Edge table: HLT → HLGT
    hlgt_to_soc: pl.DataFrame  # Edge table: HLGT → SOC


# ---------------------------------------------------------------------------
# Vocabulary helpers
# ---------------------------------------------------------------------------

def _vocab_dir(ctx: PipelineContext) -> Path:
    return _vocab_dir_shared(ctx, stage_tag="S06b")


def _load_concept_relationship(
    relationship_path: Path,
    meddra_concept_ids: list[int],
) -> pl.DataFrame:
    """Load CONCEPT_RELATIONSHIP filtered to MedDRA-only relationships."""
    logger.info(
        f"[S05b] Loading CONCEPT_RELATIONSHIP "
        f"(filtering for {len(meddra_concept_ids):,} MedDRA concepts)..."
    )
    return (
        pl.read_csv(
            relationship_path,
            separator="\t",
            has_header=True,
            schema_overrides={
                "concept_id_1": pl.Int64,
                "concept_id_2": pl.Int64,
                "relationship_id": pl.Utf8,
            },
            quote_char=None,
            null_values="",
        )
        .select(
            pl.col("concept_id_1"),
            pl.col("concept_id_2"),
            pl.col("relationship_id"),
        )
        .filter(
            pl.col("concept_id_1").is_in(meddra_concept_ids)
            & pl.col("concept_id_2").is_in(meddra_concept_ids)
        )
    )


def _build_edge_tables(
    relationship_df: pl.DataFrame,
    pt_df: pl.DataFrame,
    hlt_df: pl.DataFrame,
    hlgt_df: pl.DataFrame,
    soc_df: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Build directed edge tables for PT→HLT, HLT→HLGT, HLGT→SOC."""
    pt_ids   = pt_df["pt_id"].to_list()
    hlt_ids  = hlt_df["hlt_id"].to_list()
    hlgt_ids = hlgt_df["hlgt_id"].to_list()
    soc_ids  = soc_df["soc_id"].to_list()

    logger.info("[S05b] Building edge tables...")

    pt_to_hlt = (
        relationship_df
        .filter(
            pl.col("concept_id_1").is_in(pt_ids)
            & pl.col("concept_id_2").is_in(hlt_ids)
        )
        .select(
            pl.col("concept_id_1").alias("pt_id"),
            pl.col("concept_id_2").alias("hlt_id"),
        )
        .unique()
    )

    hlt_to_hlgt = (
        relationship_df
        .filter(
            pl.col("concept_id_1").is_in(hlt_ids)
            & pl.col("concept_id_2").is_in(hlgt_ids)
        )
        .select(
            pl.col("concept_id_1").alias("hlt_id"),
            pl.col("concept_id_2").alias("hlgt_id"),
        )
        .unique()
    )

    hlgt_to_soc = (
        relationship_df
        .filter(
            pl.col("concept_id_1").is_in(hlgt_ids)
            & pl.col("concept_id_2").is_in(soc_ids)
        )
        .select(
            pl.col("concept_id_1").alias("hlgt_id"),
            pl.col("concept_id_2").alias("soc_id"),
        )
        .unique()
    )

    logger.info(
        f"[S05b] Edge tables: {pt_to_hlt.height:,} PT→HLT, "
        f"{hlt_to_hlgt.height:,} HLT→HLGT, {hlgt_to_soc.height:,} HLGT→SOC"
    )
    return pt_to_hlt, hlt_to_hlgt, hlgt_to_soc


def _build_meddra_resources(concept_dir: Path) -> MedDRAResources:
    """Load and build all MedDRA hierarchy resources."""
    concept_path = concept_dir / "CONCEPT.csv"
    relationship_path = concept_dir / "CONCEPT_RELATIONSHIP.csv"

    logger.info("[S05b] Loading CONCEPT.csv...")
    concept_df = _load_concepts(concept_path)

    pt_df   = _concept_subset(concept_df, "PT",   "pt")
    hlt_df  = _concept_subset(concept_df, "HLT",  "hlt")
    hlgt_df = _concept_subset(concept_df, "HLGT", "hlgt")
    soc_df  = _concept_subset(concept_df, "SOC",  "soc")

    meddra_concept_ids = concept_df["concept_id"].to_list()
    relationship_df = _load_concept_relationship(relationship_path, meddra_concept_ids)
    pt_to_hlt, hlt_to_hlgt, hlgt_to_soc = _build_edge_tables(
        relationship_df, pt_df, hlt_df, hlgt_df, soc_df
    )

    logger.info(
        f"[S05b] Loaded {pt_df.height:,} PT, {hlt_df.height:,} HLT, "
        f"{hlgt_df.height:,} HLGT, {soc_df.height:,} SOC concepts"
    )
    return MedDRAResources(
        pt=pt_df, hlt=hlt_df, hlgt=hlgt_df, soc=soc_df,
        pt_to_hlt=pt_to_hlt, hlt_to_hlgt=hlt_to_hlgt, hlgt_to_soc=hlgt_to_soc,
    )


# ---------------------------------------------------------------------------
# Hierarchy path builder
# ---------------------------------------------------------------------------

def _build_full_paths(resources: MedDRAResources) -> pl.DataFrame:
    """Join all levels to produce expanded PT→HLT→HLGT→SOC rows."""
    logger.info("[S05b] Building full hierarchy paths...")

    pt_hlt = (
        resources.pt
        .join(resources.pt_to_hlt, on="pt_id", how="left")
        .join(resources.hlt, left_on="hlt_id", right_on="hlt_id", how="left")
        .select(["pt_id", "pt_code", "pt_name", "hlt_id", "hlt_code", "hlt_name"])
    )

    pt_hlt_hlgt = (
        pt_hlt
        .join(resources.hlt_to_hlgt, on="hlt_id", how="left")
        .join(resources.hlgt, left_on="hlgt_id", right_on="hlgt_id", how="left")
        .select([
            "pt_id", "pt_code", "pt_name",
            "hlt_id", "hlt_code", "hlt_name",
            "hlgt_id", "hlgt_code", "hlgt_name",
        ])
    )

    paths = (
        pt_hlt_hlgt
        .join(resources.hlgt_to_soc, on="hlgt_id", how="left")
        .join(resources.soc, left_on="soc_id", right_on="soc_id", how="left")
        .select([
            "pt_id", "pt_code", "pt_name",
            "hlt_id", "hlt_code", "hlt_name",
            "hlgt_id", "hlgt_code", "hlgt_name",
            "soc_id", "soc_code", "soc_name",
        ])
        .filter(pl.col("pt_id").is_not_null())
        .rename({
            "pt_id":   "meddra_concept_id",
            "pt_code": "meddra_concept_code",
            "pt_name": "meddra_concept_name",
            "hlt_id":   "meddra_hlt_id",
            "hlt_code": "meddra_hlt_code",
            "hlt_name": "meddra_hlt_name",
            "hlgt_id":   "meddra_hlgt_id",
            "hlgt_code": "meddra_hlgt_code",
            "hlgt_name": "meddra_hlgt_name",
            "soc_id":   "meddra_soc_id",
            "soc_code": "meddra_soc_code",
            "soc_name": "meddra_soc_name",
        })
        .sort(["meddra_concept_id", "meddra_hlt_id", "meddra_hlgt_id", "meddra_soc_id"])
    )

    logger.info(
        f"[S05b] Built {paths.height:,} full paths "
        f"({paths['meddra_concept_id'].n_unique():,} unique PTs)"
    )
    return paths


def _aggregate_by_pt(paths_df: pl.DataFrame) -> pl.DataFrame:
    """Aggregate expanded paths into a PT-centric dictionary with list columns."""
    logger.info("[S05b] Aggregating paths by PT...")

    pt_dict = (
        paths_df
        .group_by(["meddra_concept_id", "meddra_concept_code", "meddra_concept_name"])
        .agg([
            pl.col("meddra_hlt_id").drop_nulls().unique().alias("meddra_hlt_ids"),
            pl.col("meddra_hlt_code").drop_nulls().unique().alias("meddra_hlt_codes"),
            pl.col("meddra_hlt_name").drop_nulls().unique().alias("meddra_hlt_names"),
            pl.col("meddra_hlgt_id").drop_nulls().unique().alias("meddra_hlgt_ids"),
            pl.col("meddra_hlgt_code").drop_nulls().unique().alias("meddra_hlgt_codes"),
            pl.col("meddra_hlgt_name").drop_nulls().unique().alias("meddra_hlgt_names"),
            pl.col("meddra_soc_id").drop_nulls().unique().alias("meddra_soc_ids"),
            pl.col("meddra_soc_code").drop_nulls().unique().alias("meddra_soc_codes"),
            pl.col("meddra_soc_name").drop_nulls().unique().alias("meddra_soc_names"),
        ])
        .sort("meddra_concept_name")
        .rename({"meddra_concept_name": "MedDRA_concept_name"})
    )

    logger.info(f"[S05b] Aggregated to {pt_dict.height:,} unique PTs")
    return pt_dict


# ---------------------------------------------------------------------------
# Per-cohort mapping
# ---------------------------------------------------------------------------

def _map_cohort_adr(
    adr_path: Path,
    resources: MedDRAResources,
    export_dir: Path,
    cohort: str,
) -> Dict[str, int]:
    """Map ADR terms to the full MedDRA hierarchy for one cohort.

    Steps:
      0. Deduplicate reaction terms (reduces data volume)
      1. Map reaction_meddrapt → PT via Title Case
      2. Build PT→HLT→HLGT→SOC paths (vocabulary-level)
      3. Filter excluded SOC categories (3 categories)
      4. Aggregate by PT → dictionary + write expanded paths
    """
    cohort_dir = export_dir / cohort
    cohort_dir.mkdir(parents=True, exist_ok=True)

    # ── Step 0: deduplicate reaction terms ───────────────────────────────────
    logger.info(f"[S05b][{cohort.upper()}] Step 0/4: Deduplicating reaction terms...")
    adr_lf = (
        pl.scan_parquet(adr_path)
        .select(pl.col("reaction_meddrapt").cast(pl.Utf8, strict=False))
        .with_columns(
            pl.col("reaction_meddrapt").fill_null("").str.strip_chars(),
            pl.col("reaction_meddrapt").str.to_titlecase().alias("reaction_term"),
        )
        .unique(subset=["reaction_meddrapt"])
    )
    adr_unique = adr_lf.collect(streaming=True)
    adr_total  = (
        pl.scan_parquet(adr_path).select(pl.len()).collect(streaming=True).item()
    )
    adr_unique_count = adr_unique.height
    reduction_pct = (1 - adr_unique_count / adr_total) * 100 if adr_total > 0 else 0.0
    logger.info(
        f"[S05b][{cohort.upper()}]   → {adr_total:,} → {adr_unique_count:,} unique terms "
        f"({reduction_pct:.2f}% reduction)"
    )

    # ── Step 1: map terms → PT ───────────────────────────────────────────────
    logger.info(f"[S05b][{cohort.upper()}] Step 1/4: Mapping reaction terms → MedDRA PT...")
    mapped_pt = (
        adr_unique.lazy()
        .join(resources.pt.lazy(), left_on="reaction_term", right_on="pt_name_match", how="left")
        .collect(streaming=True)
    )
    mapped_count = mapped_pt.filter(pl.col("pt_id").is_not_null()).height
    coverage_pct = mapped_count / adr_unique_count * 100 if adr_unique_count > 0 else 0.0

    # ── Step 2: build full paths (vocabulary-level, independent of cohort) ───
    logger.info(f"[S05b][{cohort.upper()}] Step 2/4: Building PT→HLT→HLGT→SOC paths...")
    all_paths = _build_full_paths(resources)

    # ── Step 3: filter excluded SOCs ─────────────────────────────────────────
    logger.info(f"[S05b][{cohort.upper()}] Step 3/4: Filtering excluded SOCs...")
    paths_filtered = all_paths.filter(~pl.col("meddra_soc_name").is_in(EXCLUDED_SOCS))
    soc_filtered_count = all_paths.height - paths_filtered.height
    logger.info(
        f"[S05b][{cohort.upper()}]   → Removed {soc_filtered_count:,} paths "
        f"(excluded SOCs: {', '.join(EXCLUDED_SOCS)})"
    )

    # Restrict to PTs that were actually found in this cohort's ADR data
    pt_ids_found = mapped_pt.filter(pl.col("pt_id").is_not_null())["pt_id"].unique().to_list()
    paths_filtered = paths_filtered.filter(
        pl.col("meddra_concept_id").is_in(pt_ids_found)
    )

    # ── Step 4: aggregate + write ────────────────────────────────────────────
    logger.info(f"[S05b][{cohort.upper()}] Step 4/4: Aggregating and writing outputs...")
    pt_hierarchy_dict = _aggregate_by_pt(paths_filtered)

    dict_path  = cohort_dir / "pt_hierarchy_dictionary_full_data.parquet"
    paths_path = cohort_dir / "pt_hierarchy_paths_full_data.parquet"

    pt_hierarchy_dict.write_parquet(dict_path,  compression="zstd")
    paths_filtered.write_parquet(paths_path, compression="zstd")

    unique_pts   = pt_hierarchy_dict.height
    unique_hlts  = pt_hierarchy_dict["meddra_hlt_names"].explode().drop_nulls().n_unique()
    unique_hlgts = pt_hierarchy_dict["meddra_hlgt_names"].explode().drop_nulls().n_unique()
    unique_socs  = pt_hierarchy_dict["meddra_soc_names"].explode().drop_nulls().n_unique()

    logger.success(
        f"[S05b][{cohort.upper()}] ✓ {mapped_count:,}/{adr_unique_count:,} unique ADRs mapped "
        f"({coverage_pct:.2f}%) | {unique_pts:,} PTs · {unique_hlts} HLTs · "
        f"{unique_hlgts} HLGTs · {unique_socs} SOCs"
    )
    logger.info(f"[S05b][{cohort.upper()}]   Dictionary : {dict_path}")
    logger.info(f"[S05b][{cohort.upper()}]   Paths      : {paths_path}")

    return {
        "adr_rows":              int(adr_total),
        "adr_unique_rows":       int(adr_unique_count),
        "mapped_rows":           int(mapped_count),
        "coverage_pct":          coverage_pct,
        "unique_pts":            int(unique_pts),
        "unique_hlts":           int(unique_hlts),
        "unique_hlgts":          int(unique_hlgts),
        "unique_socs":           int(unique_socs),
    }


# ---------------------------------------------------------------------------
# Stage entrypoint
# ---------------------------------------------------------------------------

def run(ctx: PipelineContext) -> None:
    vocab_dir  = _vocab_dir(ctx)
    resources  = _build_meddra_resources(vocab_dir)

    input_dir  = stage_output_path(ctx, "s04_split_adr")
    output_dir = stage_output_path(ctx, "s05b_map_omop_meddra_full_hierarchy")
    output_dir.mkdir(parents=True, exist_ok=True)

    manifest_payload: Dict[str, Dict[str, object]] = {}

    logger.info("=" * 80)
    logger.info("[S05b] MedDRA full-hierarchy mapping (adult + pediatric)")
    logger.info("=" * 80)

    for cohort in ("pediatric", "adult"):
        logger.info("")
        logger.info("┌" + "─" * 78 + "┐")
        logger.info(f"│ Cohort: {cohort.upper():<71} │")
        logger.info("└" + "─" * 78 + "┘")

        adr_path = input_dir / f"{cohort}_adr_full_data.parquet"
        if not adr_path.exists():
            raise FileNotFoundError(
                f"[S05b] Missing ADR file for cohort '{cohort}': {adr_path}"
            )

        stats = _map_cohort_adr(
            adr_path=adr_path,
            resources=resources,
            export_dir=output_dir,
            cohort=cohort,
        )

        dq_summary_markdown(
            ctx,
            "s05b_map_omop_meddra_full_hierarchy",
            cohort,
            stats["adr_rows"],
            stats["mapped_rows"],
            {"meddra_coverage_pct": f"{stats['coverage_pct']:.2f}"},
        )

        manifest_payload[cohort] = {
            **stats,
            "dictionary_path": str(output_dir / cohort / "pt_hierarchy_dictionary_full_data.parquet"),
            "paths_path":      str(output_dir / cohort / "pt_hierarchy_paths_full_data.parquet"),
        }

    # ── Summary ──────────────────────────────────────────────────────────────
    logger.info("")
    logger.info("=" * 80)
    logger.info("[S05b] Summary")
    logger.info("=" * 80)
    logger.info(
        "┌────────────┬──────────────┬──────────────┬────────────┬──────┬──────┬───────┬──────┐"
    )
    logger.info(
        "│ Cohort     │ Input ADRs   │ Mapped ADRs  │ Coverage   │  PTs │ HLTs │ HLGTs │ SOCs │"
    )
    logger.info(
        "├────────────┼──────────────┼──────────────┼────────────┼──────┼──────┼───────┼──────┤"
    )
    for cohort in ("pediatric", "adult"):
        s = manifest_payload[cohort]
        logger.info(
            f"│ {cohort:<10} │ {s['adr_rows']:>12,} │ {s['mapped_rows']:>12,} │ "
            f"{s['coverage_pct']:>9.2f}% │ {s['unique_pts']:>4} │ {s['unique_hlts']:>4} │ "
            f"{s['unique_hlgts']:>5} │ {s['unique_socs']:>4} │"
        )
    logger.info(
        "└────────────┴──────────────┴──────────────┴────────────┴──────┴──────┴───────┴──────┘"
    )
    logger.info("")
    logger.info(f"  Excluded SOCs : {', '.join(EXCLUDED_SOCS)}")
    logger.info(f"  Reaction filter: NONE (all terms kept)")
    logger.info(f"  Hierarchy      : PT → HLT → HLGT → SOC  (4 levels via CONCEPT_RELATIONSHIP)")
    logger.info("")

    vocab_version = get_vocab_version_info(vocab_dir)
    logger.info(f"[S05b] Vocabulary version info: {vocab_version}")

    write_manifest(
        ctx,
        "s05b_map_omop_meddra_full_hierarchy",
        {
            "stage": "s05b_map_omop_meddra_full_hierarchy",
            "vocabulary": vocab_version,
            "cohorts": manifest_payload,
        },
    )
    logger.success("[S05b] Full-hierarchy MedDRA mapping complete")


__all__ = ["run"]
