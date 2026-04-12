#!/usr/bin/env python3
"""Create a small test sample from real S02 output for pipeline validation.

Run this on SLURM (or wherever the full S02 output exists) to create a
~1,000-report subset that can be used to test S03-S10 without waiting hours.

Usage:
    cd /path/to/fears_dataset
    python scripts/create_test_sample.py [--source-dir data/staging/s02_entity_format] [--n-reports 1000]

Output:
    data/test_sample/s02_entity_format/*.parquet   (6 ER tables, filtered)
    data/test_sample/manifest.json                 (row counts per table)
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path

import polars as pl


# The 6 ER tables that S03 reads from S02 output
ER_TABLES = [
    "patient",
    "report",
    "report_serious",
    "reporter",
    "drugcharacteristics_extended",
    "reactions",
]

# Optional tables that some stages may use
OPTIONAL_TABLES = [
    "drug_mapping_input",
    "drug_openfda_wide",
]


def create_sample(
    source_dir: Path,
    output_dir: Path,
    n_reports: int = 1000,
    seed: int = 42,
) -> dict:
    """Sample n_reports from the full S02 output and write filtered parquets.

    Strategy:
      1. Read patient.parquet, pick n_reports unique safetyreportids at random
      2. Filter all 6 ER tables to only those report IDs
      3. Write filtered parquets to output_dir

    Returns a manifest dict with row counts per table.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # Step 1: Sample report IDs from patient table
    # Use streaming to avoid OOM on large parquets (patient.parquet can be 10+ GB)
    patient_path = source_dir / "patient.parquet"
    if not patient_path.exists():
        print(f"ERROR: {patient_path} not found. Is source_dir correct?", file=sys.stderr)
        sys.exit(1)

    print(f"Reading patient table from {patient_path} (streaming)...")

    # Only read the safetyreportid column to minimize memory usage
    total_unique = (
        pl.scan_parquet(patient_path)
        .select(pl.col("safetyreportid"))
        .unique()
        .select(pl.len())
        .collect(streaming=True)
        .item()
    )
    print(f"  Total unique reports: {total_unique:,}")

    # Sample by reading a small head and picking random IDs
    # Strategy: read only the ID column, take a larger slice, then sample in-memory
    sample_n = min(n_reports, total_unique)
    # Read just the ID column with streaming, shuffle via hash, take top N
    sampled_ids = (
        pl.scan_parquet(patient_path)
        .select(pl.col("safetyreportid").cast(pl.Utf8))
        .unique()
        .with_columns(
            # Deterministic pseudo-random sort using hash + seed
            (pl.col("safetyreportid").hash(seed=seed)).alias("_hash")
        )
        .sort("_hash")
        .head(sample_n)
        .select("safetyreportid")
        .collect(streaming=True)
    )
    report_id_list = sampled_ids["safetyreportid"].to_list()
    print(f"  Sampled {len(report_id_list):,} reports (seed={seed})")
    del sampled_ids  # free memory

    # Step 2: Filter each ER table and write (one at a time to keep memory low)
    manifest = {"n_reports_sampled": len(report_id_list), "seed": seed, "tables": {}}

    # Convert to a set for fast lookup and a small Series for Polars is_in
    report_id_set = set(report_id_list)

    for table_name in ER_TABLES + OPTIONAL_TABLES:
        src_path = source_dir / f"{table_name}.parquet"
        dst_path = output_dir / f"{table_name}.parquet"

        if not src_path.exists():
            if table_name in ER_TABLES:
                print(f"  WARNING: Required table {table_name}.parquet not found!")
            else:
                print(f"  SKIP: Optional table {table_name}.parquet not found")
            continue

        print(f"  Filtering {table_name}...", end=" ", flush=True)

        # Check if table has safetyreportid column
        schema = pl.scan_parquet(src_path).collect_schema()
        if "safetyreportid" not in schema.names():
            # Table doesn't have safetyreportid — copy as-is (e.g., reference tables)
            print(f"(no safetyreportid, copying full table)", flush=True)
            shutil.copy2(src_path, dst_path)
            row_count = pl.scan_parquet(dst_path).select(pl.len()).collect(streaming=True).item()
        else:
            # Filter by sampled report IDs using streaming to avoid OOM
            filtered = (
                pl.scan_parquet(src_path)
                .with_columns(pl.col("safetyreportid").cast(pl.Utf8, strict=False))
                .filter(pl.col("safetyreportid").is_in(report_id_list))
                .collect(streaming=True)
            )
            filtered.write_parquet(dst_path, compression="zstd")
            row_count = filtered.height
            del filtered  # free memory immediately

        manifest["tables"][table_name] = {
            "rows": row_count,
            "path": str(dst_path),
        }
        print(f"{row_count:,} rows")

    # Step 3: Write manifest
    manifest_path = output_dir.parent / "test_sample_manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)
    print(f"\nManifest written to {manifest_path}")

    return manifest


def link_vocab(source_vocab_dir: Path, output_vocab_dir: Path) -> None:
    """Symlink essential OMOP vocabulary files for MedDRA mapping (S06/S06b).

    Uses symlinks instead of copying to avoid OOM and save disk space.
    Vocabulary files can be several GB each.
    """
    vocab_subdir = "vocabulary_SNOMED_MEDDRA_RxNorm_ATC"
    src = source_vocab_dir / vocab_subdir
    dst = output_vocab_dir / vocab_subdir

    if not src.exists():
        print(f"WARNING: Vocabulary not found at {src} — S06/S06b will be skipped in tests")
        return

    essential_files = [
        "CONCEPT.csv",
        "CONCEPT_ANCESTOR.csv",
        "CONCEPT_RELATIONSHIP.csv",
    ]

    dst.mkdir(parents=True, exist_ok=True)
    for fname in essential_files:
        src_file = src / fname
        dst_file = dst / fname
        if src_file.exists():
            size_mb = src_file.stat().st_size / (1024 * 1024)
            if dst_file.exists() or dst_file.is_symlink():
                dst_file.unlink()
            dst_file.symlink_to(src_file.resolve())
            print(f"  Linked {fname} ({size_mb:.0f} MB)")
        else:
            print(f"  WARNING: {fname} not found in {src}")


def main():
    parser = argparse.ArgumentParser(description="Create test sample from S02 output")
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=Path("data/staging/s02_entity_format"),
        help="Path to full S02 output (default: data/staging/s02_entity_format)",
    )
    parser.add_argument(
        "--vocab-dir",
        type=Path,
        default=Path("data/vocab"),
        help="Path to vocabulary directory (default: data/vocab)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/test_sample/s02_entity_format"),
        help="Where to write the sample (default: data/test_sample/s02_entity_format)",
    )
    parser.add_argument(
        "--n-reports",
        type=int,
        default=1000,
        help="Number of reports to sample (default: 1000)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility (default: 42)",
    )
    parser.add_argument(
        "--copy-vocab",
        action="store_true",
        help="Also copy essential vocabulary files for S06/S06b testing",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("CREATE TEST SAMPLE")
    print("=" * 60)
    print(f"Source:    {args.source_dir}")
    print(f"Output:    {args.output_dir}")
    print(f"Reports:   {args.n_reports}")
    print(f"Seed:      {args.seed}")
    print()

    manifest = create_sample(
        source_dir=args.source_dir,
        output_dir=args.output_dir,
        n_reports=args.n_reports,
        seed=args.seed,
    )

    if args.copy_vocab:
        print("\n--- Linking vocabulary files ---")
        output_vocab = args.output_dir.parent / "vocab"
        link_vocab(args.vocab_dir, output_vocab)

    print("\n" + "=" * 60)
    print("DONE — Sample data ready for testing")
    print("=" * 60)
    print(f"\nTo run the test pipeline:")
    print(f"  bash scripts/run_test_pipeline.sh")


if __name__ == "__main__":
    main()
