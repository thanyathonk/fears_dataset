#!/usr/bin/env python3
"""Diagnose why openfda CSV files are skipped in S02.

Usage:
  python scripts/diagnose_openfda_skipped.py [openfda_dir] [--sample N]
  # Default dir: data/openFDA_drug_event/patient_drug_openfda
  # --sample N: check only first N files (faster for large dirs)

  S02_OPENFDA_DIAGNOSE=1 python -m src.stages.s02_entity_format_stream ...
  # Alternative: run full S02 and get inline diagnostics when files are skipped
"""

import argparse
from pathlib import Path
import sys

# project root
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.stages.s02_entity_format_stream import (
    OPENFDA_KNOWN_KEYS,
    _diagnose_openfda_file,
    _enumerate_csvs,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Diagnose why openfda CSV files are skipped in S02")
    parser.add_argument("openfda_dir", nargs="?", default=None, help="Path to patient_drug_openfda")
    parser.add_argument("--sample", type=int, default=0, help="Check only first N files (0=all)")
    args = parser.parse_args()

    if args.openfda_dir:
        openfda_dir = Path(args.openfda_dir).resolve()
    else:
        openfda_dir = ROOT / "data" / "openFDA_drug_event" / "patient_drug_openfda"

    if not openfda_dir.is_dir():
        print(f"Error: {openfda_dir} is not a directory", file=sys.stderr)
        sys.exit(1)

    all_files = _enumerate_csvs(openfda_dir)
    files = all_files[: args.sample] if args.sample else all_files
    if args.sample:
        print(f"Checking first {len(files)} of {len(all_files)} files ...\n")
    else:
        print(f"Checking {len(files)} files in {openfda_dir.name}/ ...\n")

    reasons: dict[str, list[str]] = {}
    for fp in files:
        reason = _diagnose_openfda_file(Path(fp), OPENFDA_KNOWN_KEYS)
        key = reason or "ok"
        reasons.setdefault(key, []).append(Path(fp).name)

    n_ok = len(reasons.get("ok", []))
    n_skip = len(files) - n_ok
    print(f"OK: {n_ok}  |  Skipped: {n_skip}\n")

    if n_skip == 0:
        return

    print("Skipped file reasons (with example filenames):")
    for reason in sorted(reasons.keys()):
        if reason == "ok":
            continue
        names = reasons[reason]
        examples = names[:5] if len(names) > 5 else names
        print(f"  {reason}: {len(names)} file(s)")
        print(f"    e.g. {examples}")


if __name__ == "__main__":
    main()
