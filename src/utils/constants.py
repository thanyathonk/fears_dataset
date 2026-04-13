"""Shared constants used across multiple pipeline stages.

Centralised here to avoid duplication and ensure consistency.
"""

from __future__ import annotations

# Reporter types excluded from quality-filtered cohorts.
# Applied in S03 (cohort build) and S09 (final merge).
EXCLUDED_REPORTERS = [
    "Unknown",
    "Lawyer",
    "Consumer or non-health professional",
]
