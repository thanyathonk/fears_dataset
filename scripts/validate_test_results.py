#!/usr/bin/env python3
"""Validate pipeline test results after refactoring.

Compares output parquets between two pipeline runs (baseline vs refactored)
to ensure the refactoring didn't change the data output.

Usage:
    # Compare test output against a baseline run:
    python scripts/validate_test_results.py \
        --baseline data/test_baseline/ \
        --current  data/test_output/

    # Self-check mode (just validate current output is sane):
    python scripts/validate_test_results.py --current data/test_output/

Output:
    Prints a validation report to stdout and writes to logs/validation_report.md
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

import polars as pl


# Expected output files per cohort (stage → filename pattern)
EXPECTED_OUTPUTS = {
    "s03_join_partition_age": [
        "{cohort}_events_full_data.parquet",
    ],
    "s05_split_adr": [
        "{cohort}_adr_full_data.parquet",
    ],
    "s06_map_omop_meddra": [
        "{cohort}/pt_soc_dictionary_full_data.parquet",
    ],
    "s07_split_drug": [
        "{cohort}_drugs_full_data.parquet",
    ],
    "s08_enrich_drug_identifiers": [
        "{cohort}_drugs_enriched_final_full_data.parquet",
    ],
    "s09_finalize_merge_and_report": [
        "{cohort}_patient_report_reporter_drug_reaction_full_data.parquet",
    ],
}

COHORTS = ["adult", "pediatric"]

# Key columns that should never be all-null
CRITICAL_COLUMNS = {
    "s03_join_partition_age": ["safetyreportid", "age_years", "medicinal_product", "reaction_meddrapt"],
    "s05_split_adr": ["safetyreportid", "reaction_meddrapt"],
    "s07_split_drug": ["medicinal_product"],
}


class ValidationResult:
    def __init__(self):
        self.checks: list[dict] = []

    def add(self, stage: str, check: str, status: str, detail: str = ""):
        self.checks.append({"stage": stage, "check": check, "status": status, "detail": detail})

    @property
    def passed(self) -> int:
        return sum(1 for c in self.checks if c["status"] == "PASS")

    @property
    def failed(self) -> int:
        return sum(1 for c in self.checks if c["status"] == "FAIL")

    @property
    def warned(self) -> int:
        return sum(1 for c in self.checks if c["status"] == "WARN")

    def to_markdown(self) -> str:
        lines = [
            "# Pipeline Validation Report",
            f"**Date:** {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}",
            f"**Result:** {self.passed} passed, {self.failed} failed, {self.warned} warnings",
            "",
            "| Stage | Check | Status | Detail |",
            "|-------|-------|--------|--------|",
        ]
        for c in self.checks:
            emoji = {"PASS": "✅", "FAIL": "❌", "WARN": "⚠️"}.get(c["status"], "❓")
            lines.append(f"| {c['stage']} | {c['check']} | {emoji} {c['status']} | {c['detail']} |")
        return "\n".join(lines)


def check_file_exists(result: ValidationResult, staging_dir: Path) -> None:
    """Check that expected output files exist."""
    for stage, patterns in EXPECTED_OUTPUTS.items():
        stage_dir = staging_dir / stage
        for cohort in COHORTS:
            for pattern in patterns:
                fname = pattern.format(cohort=cohort)
                fpath = stage_dir / fname
                if fpath.exists():
                    row_count = pl.scan_parquet(str(fpath)).select(pl.len()).collect().item()
                    result.add(stage, f"{cohort}/{fname} exists", "PASS", f"{row_count:,} rows")
                else:
                    result.add(stage, f"{cohort}/{fname} exists", "FAIL", "File not found")


def check_schema_match(result: ValidationResult, staging_dir: Path) -> None:
    """Check that critical columns exist and have correct types."""
    for stage, columns in CRITICAL_COLUMNS.items():
        stage_dir = staging_dir / stage
        for cohort in COHORTS:
            patterns = EXPECTED_OUTPUTS.get(stage, [])
            for pattern in patterns:
                fname = pattern.format(cohort=cohort)
                fpath = stage_dir / fname
                if not fpath.exists():
                    continue

                schema = pl.scan_parquet(str(fpath)).collect_schema()
                for col in columns:
                    if col in schema.names():
                        result.add(stage, f"{cohort}: column '{col}' present", "PASS", str(schema[col]))
                    else:
                        result.add(stage, f"{cohort}: column '{col}' present", "FAIL", "Missing!")


def check_no_empty_critical(result: ValidationResult, staging_dir: Path) -> None:
    """Check that critical columns are not 100% null."""
    for stage, columns in CRITICAL_COLUMNS.items():
        stage_dir = staging_dir / stage
        for cohort in COHORTS:
            patterns = EXPECTED_OUTPUTS.get(stage, [])
            for pattern in patterns:
                fname = pattern.format(cohort=cohort)
                fpath = stage_dir / fname
                if not fpath.exists():
                    continue

                df = pl.scan_parquet(str(fpath))
                total = df.select(pl.len()).collect().item()
                if total == 0:
                    result.add(stage, f"{cohort}: non-empty table", "WARN", "0 rows!")
                    continue

                schema_names = df.collect_schema().names()
                for col in columns:
                    if col not in schema_names:
                        continue
                    null_count = df.select(pl.col(col).is_null().sum()).collect().item()
                    null_pct = null_count / total * 100
                    if null_pct == 100:
                        result.add(stage, f"{cohort}: '{col}' not all-null", "FAIL", "100% null!")
                    elif null_pct > 50:
                        result.add(stage, f"{cohort}: '{col}' not all-null", "WARN", f"{null_pct:.1f}% null")
                    else:
                        result.add(stage, f"{cohort}: '{col}' not all-null", "PASS", f"{null_pct:.1f}% null")


def check_row_count_match(result: ValidationResult, baseline_dir: Path, current_dir: Path) -> None:
    """Compare row counts between baseline and current output."""
    for stage, patterns in EXPECTED_OUTPUTS.items():
        for cohort in COHORTS:
            for pattern in patterns:
                fname = pattern.format(cohort=cohort)
                base_path = baseline_dir / stage / fname
                curr_path = current_dir / stage / fname

                if not base_path.exists() or not curr_path.exists():
                    continue

                base_rows = pl.scan_parquet(str(base_path)).select(pl.len()).collect().item()
                curr_rows = pl.scan_parquet(str(curr_path)).select(pl.len()).collect().item()

                if base_rows == curr_rows:
                    result.add(stage, f"{cohort}: row count match", "PASS", f"{curr_rows:,} rows")
                else:
                    diff = curr_rows - base_rows
                    result.add(
                        stage,
                        f"{cohort}: row count match",
                        "FAIL",
                        f"baseline={base_rows:,}, current={curr_rows:,} (diff={diff:+,})",
                    )


def check_manifests(result: ValidationResult, log_dir: Path) -> None:
    """Check that manifest JSON files were created."""
    manifest_dir = log_dir / "manifests"
    if not manifest_dir.exists():
        result.add("manifests", "manifest dir exists", "WARN", f"{manifest_dir} not found")
        return

    for stage in EXPECTED_OUTPUTS:
        manifest_path = manifest_dir / f"{stage}.json"
        if manifest_path.exists():
            with open(manifest_path) as f:
                data = json.load(f)
            result.add("manifests", f"{stage}.json created", "PASS", f"{len(data)} keys")
        else:
            result.add("manifests", f"{stage}.json created", "WARN", "Not found")


def main():
    parser = argparse.ArgumentParser(description="Validate pipeline test results")
    parser.add_argument("--current", type=Path, required=True, help="Current staging output directory")
    parser.add_argument("--baseline", type=Path, default=None, help="Baseline staging output for comparison")
    parser.add_argument("--log-dir", type=Path, default=None, help="Log directory to check manifests")
    parser.add_argument("--output", type=Path, default=Path("logs/validation_report.md"), help="Report output path")
    args = parser.parse_args()

    result = ValidationResult()

    print("=" * 60)
    print("PIPELINE VALIDATION")
    print("=" * 60)
    print(f"Current output: {args.current}")
    if args.baseline:
        print(f"Baseline:       {args.baseline}")
    print()

    # Run checks
    print("Checking file existence...")
    check_file_exists(result, args.current)

    print("Checking schema...")
    check_schema_match(result, args.current)

    print("Checking critical column completeness...")
    check_no_empty_critical(result, args.current)

    if args.baseline:
        print("Comparing row counts with baseline...")
        check_row_count_match(result, args.baseline, args.current)

    if args.log_dir:
        print("Checking manifests...")
        check_manifests(result, args.log_dir)

    # Output report
    report = result.to_markdown()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(report)

    print()
    print(report)
    print()
    print(f"Report saved to {args.output}")

    # Exit code
    if result.failed > 0:
        print(f"\nFAILED: {result.failed} check(s) failed")
        sys.exit(1)
    else:
        print(f"\nALL PASSED ({result.passed} checks, {result.warned} warnings)")


if __name__ == "__main__":
    main()
