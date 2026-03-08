#!/usr/bin/env python3
"""Reproduce baseline drug coverage analysis from actual project files.

Usage:
  python scripts/analyze_baseline_coverage.py

Output: Prints coverage tables and top unmapped to stdout.
See BASELINE_DRUG_COVERAGE_REPORT.md for full report.
"""
from pathlib import Path

import polars as pl

BASE = Path(__file__).resolve().parent.parent
STAGING = BASE / "data" / "staging"


def main():
    def record_level_mapped(events_path: Path, enriched_path: Path) -> tuple[int, int]:
        enr = pl.read_parquet(enriched_path)
        enr = enr.filter(
            pl.col("rxcui").is_not_null()
            & (pl.col("rxcui").cast(pl.Utf8).str.strip_chars().str.len_chars() > 0)
        )
        mapped_mp = enr.select("medicinal_product").unique().to_series().to_list()
        ev = pl.scan_parquet(events_path)
        ev_mapped = ev.filter(pl.col("medicinal_product").is_in(mapped_mp))
        count = ev_mapped.select(pl.len()).collect().item()
        total = pl.scan_parquet(events_path).select(pl.len()).collect().item()
        return count, total

    def top_unmapped(cohort: str, n: int = 30) -> pl.DataFrame:
        events_path = STAGING / f"s03_join_partition_age/{cohort}_events_full_data.parquet"
        enriched_path = STAGING / f"s08_enrich_drug_identifiers/{cohort}_drugs_enriched_final_full_data.parquet"
        enr = pl.read_parquet(enriched_path)
        unmapped_mp = enr.filter(
            pl.col("rxcui").is_null()
            | (pl.col("rxcui").cast(pl.Utf8).str.strip_chars().str.len_chars() == 0)
        ).select("medicinal_product").to_series().to_list()
        ev = pl.scan_parquet(events_path)
        ev_unmapped = ev.filter(pl.col("medicinal_product").is_in(unmapped_mp))
        return (
            ev_unmapped.group_by("medicinal_product")
            .agg(pl.len().alias("record_count"))
            .sort("record_count", descending=True)
            .head(n)
            .collect()
        )

    print("=" * 60)
    print("BASELINE DRUG COVERAGE (from actual files)")
    print("=" * 60)

    for cohort in ["adult", "pediatric"]:
        events_path = STAGING / f"s03_join_partition_age/{cohort}_events_full_data.parquet"
        enriched_path = STAGING / f"s08_enrich_drug_identifiers/{cohort}_drugs_enriched_final_full_data.parquet"
        if not events_path.exists() or not enriched_path.exists():
            print(f"\n[{cohort}] Missing files — skip")
            continue

        ev = pl.scan_parquet(events_path)
        total = ev.select(pl.len()).collect().item()
        with_mp = ev.filter(
            pl.col("medicinal_product").is_not_null()
            & (pl.col("medicinal_product").str.len_chars() > 0)
        ).select(pl.len()).collect().item()
        unique_mp = (
            ev.select("medicinal_product")
            .filter(
                pl.col("medicinal_product").is_not_null()
                & (pl.col("medicinal_product").str.len_chars() > 0)
            )
            .unique()
            .select(pl.len())
            .collect()
            .item()
        )

        enr = pl.read_parquet(enriched_path)
        enriched_total = len(enr)
        with_rxcui = len(
            enr.filter(
                pl.col("rxcui").is_not_null()
                & (pl.col("rxcui").cast(pl.Utf8).str.strip_chars().str.len_chars() > 0)
            )
        )

        rec_mapped, _ = record_level_mapped(events_path, enriched_path)

        print(f"\n--- {cohort.upper()} ---")
        print(f"  Record total:              {total:>12,}")
        print(f"  Record with medicinal_product: {with_mp:>12,} (input 100%)")
        print(f"  Record-level mapped:       {rec_mapped:>12,} ({100*rec_mapped/total:.2f}%)")
        print(f"  Unique medicinal_products: {unique_mp:>12,}")
        print(f"  Unique with RxCUI:         {with_rxcui:>12,} ({100*with_rxcui/unique_mp:.2f}%)")

    print("\n--- Top 15 unmapped (Adult) ---")
    df = top_unmapped("adult", 15)
    for r in df.iter_rows(named=True):
        print(f"  {r['record_count']:>10,}  {r['medicinal_product'][:50]}")

    print("\n--- Top 15 unmapped (Pediatric) ---")
    df = top_unmapped("pediatric", 15)
    for r in df.iter_rows(named=True):
        print(f"  {r['record_count']:>10,}  {r['medicinal_product'][:50]}")


if __name__ == "__main__":
    main()
