#!/usr/bin/env python3
"""Deprecated: LLM decomposition runs inside ``src.stages.s07b_llm_clean.run`` only."""

from __future__ import annotations

import sys

def main() -> None:
    print(
        "scripts/llm_clean_drugs.py is no longer used.\n"
        "Run:  python -m src.cli run-stage s07b_llm_clean\n"
        "(GPU node; set LLM_MODEL_NAME, HF_CACHE / LLM_CACHE_DIR, LLM_BATCH_SIZE as needed.)",
        file=sys.stderr,
    )
    sys.exit(2)


if __name__ == "__main__":
    main()
