from __future__ import annotations

"""Stage S07 – One row per unique ``medicinal_product`` with **list**-valued drug context.

For each ``medicinal_product``, context columns collect **all distinct** non-empty values
seen in events (sorted for stable output). No ``index`` column.

See previous doc revisions for S03 source and normalization behaviour.
"""

from pathlib import Path
from typing import Dict, List

import polars as pl
from loguru import logger
from tqdm import tqdm

from src.utils.dq import dq_summary_markdown
from src.utils.drug_name_normalize import normalize_faers_drug_name
from src.utils.io import PipelineContext, stage_output_path, write_manifest

_EVENTS_REQUIRED = ["safetyreportid", "medicinal_product"]

_CONTEXT_COLS: List[str] = [
    "active_substance_name",
    "drug_dosage_form",
    "drug_authorization_number",
    "action_drug",
]

_RENAME = {"active_substance_name": "active_substance_faers"}

_LIST_UTF8_OUT = [
    "active_substance_faers",
    "drug_dosage_form",
    "drug_authorization_number",
    "action_drug",
]


def _agg_distinct_sorted_utf8(col: str, out_alias: str) -> pl.Expr:
    return (
        pl.col(col)
        .cast(pl.Utf8, strict=False)
        .implode()
        .list.drop_nulls()
        .list.unique()
        .list.sort()
        .alias(out_alias)
    )


def _agg_distinct_sorted_entry(out_alias: str) -> pl.Expr:
    return (
        pl.col("entry")
        .cast(pl.Int64, strict=False)
        .implode()
        .list.drop_nulls()
        .list.unique()
        .list.sort()
        .alias(out_alias)
    )


def _strip_nonempty_unique_sort(col: str) -> pl.Expr:
    """Strip strings, drop empties, then unique+sort within each list."""
    return (
        pl.col(col)
        .list.eval(pl.element().str.strip_chars())
        .list.eval(pl.element().filter(pl.element().str.len_chars() > 0))
        .list.unique()
        .list.sort()
    )


def _build_unique_drug_table(events_path: Path) -> pl.DataFrame:
    schema_names = pl.scan_parquet(events_path).collect_schema().names()
    missing = [c for c in _EVENTS_REQUIRED if c not in schema_names]
    if missing:
        raise ValueError(
            f"[S07] events parquet missing columns {missing}: {events_path}\n"
            "Re-run S03 after drugcharacteristics_extended is wired in."
        )

    select_cols = list(_EVENTS_REQUIRED)
    if "entry" in schema_names:
        select_cols.append("entry")
    for c in _CONTEXT_COLS:
        if c in schema_names:
            select_cols.append(c)

    events = (
        pl.scan_parquet(events_path)
        .select(select_cols)
        .with_columns(pl.col("safetyreportid").cast(pl.Utf8, strict=False))
    )

    agg_exprs: List[pl.Expr] = []
    if "entry" in select_cols:
        agg_exprs.append(_agg_distinct_sorted_entry("drug_entries"))

    for c in _CONTEXT_COLS:
        if c not in select_cols:
            continue
        out = _RENAME.get(c, c)
        agg_exprs.append(_agg_distinct_sorted_utf8(c, out))

    if not agg_exprs:
        raise RuntimeError("[S07] No aggregations — check events schema")

    deduped = (
        events.group_by("medicinal_product", maintain_order=False)
        .agg(agg_exprs)
        .with_columns(pl.col("medicinal_product").cast(pl.Utf8, strict=False))
        .filter(
            pl.col("medicinal_product").is_not_null()
            & (pl.col("medicinal_product").str.strip_chars().str.len_chars() > 0),
        )
        .collect(streaming=True)
    )

    for c in _LIST_UTF8_OUT:
        if c not in deduped.columns:
            deduped = deduped.with_columns(pl.lit([], dtype=pl.List(pl.Utf8)).alias(c))
        else:
            deduped = deduped.with_columns(
                pl.when(pl.col(c).is_null())
                .then(pl.lit([], dtype=pl.List(pl.Utf8)))
                .otherwise(_strip_nonempty_unique_sort(c))
                .alias(c)
            )

    if "drug_entries" not in deduped.columns:
        deduped = deduped.with_columns(pl.lit([], dtype=pl.List(pl.Int64)).alias("drug_entries"))
    else:
        deduped = deduped.with_columns(
            pl.when(pl.col("drug_entries").is_null())
            .then(pl.lit([], dtype=pl.List(pl.Int64)))
            .otherwise(pl.col("drug_entries"))
            .alias("drug_entries")
        )

    mp_list = deduped.get_column("medicinal_product").to_list()
    norms = [normalize_faers_drug_name(x) for x in mp_list]
    deduped = deduped.with_columns(pl.Series("medicinal_product_norm", norms))

    return deduped.sort("medicinal_product")


def run(ctx: PipelineContext) -> None:
    source_dir = stage_output_path(ctx, "s03_join_partition_age")
    output_dir = stage_output_path(ctx, "s07_split_drug")
    output_dir.mkdir(parents=True, exist_ok=True)

    manifest_payload: Dict[str, Dict[str, object]] = {}

    for cohort in tqdm(("pediatric", "adult"), desc="S07 cohorts"):
        logger.info(f"[S07] Building unique drug table for {cohort}")

        events_path = source_dir / f"{cohort}_events_full_data.parquet"
        if not events_path.exists():
            raise FileNotFoundError(f"Missing cleaned dataset: {events_path}")

        unique_drugs = _build_unique_drug_table(events_path)

        out_path = output_dir / f"{cohort}_drugs_full_data.parquet"
        unique_drugs.write_parquet(out_path, compression="zstd")

        input_rows = (
            pl.scan_parquet(events_path)
            .select(pl.len())
            .collect(streaming=True)
            .item()
        )

        n_with_sub = int(
            unique_drugs.select((pl.col("active_substance_faers").list.len() > 0).sum()).item()
        )

        dq_summary_markdown(
            ctx,
            "s07_split_drug",
            cohort,
            input_rows,
            unique_drugs.height,
            {
                "unique_drugs": unique_drugs.height,
                "with_nonempty_active_substance_faers": n_with_sub,
            },
        )

        logger.info(
            f"[S07] {cohort}: {unique_drugs.height:,} unique medicinal_product rows "
            f"(from {input_rows:,} event rows)"
        )

        manifest_payload[cohort] = {
            "input_event_rows": input_rows,
            "unique_drugs": unique_drugs.height,
            "output_path": str(out_path),
            "columns": unique_drugs.columns,
        }

    write_manifest(
        ctx,
        "s07_split_drug",
        {"stage": "s07_split_drug", "cohorts": manifest_payload},
    )

    logger.success("[S07] Drug extraction complete")
    logger.info(
        "[S07] Next: run LLM script on {cohort}_drugs_full_data.parquet → "
        "{cohort}_drugs_llm_cleaned_full_data.parquet (basename + ingredients JSON), then S07b"
    )


__all__ = ["run"]
