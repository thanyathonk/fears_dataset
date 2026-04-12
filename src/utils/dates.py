"""Shared date parsing helpers for FAERS pipeline stages.

FAERS date columns come in multiple formats across different partitions:
  - Native Date type (already parsed by Parquet/Arrow)
  - YYYYMMDD string (e.g. "20140115")
  - YYYY-MM-DD string (e.g. "2014-01-15")

The ``parse_date_column`` function tries each format in order via ``pl.coalesce``,
returning null if none succeed (equivalent to ``errors='coerce'``).
"""

from __future__ import annotations

import polars as pl


def parse_date_column(col_name: str) -> pl.Expr:
    """Parse a date column with multiple format fallbacks.

    Attempts (in order):
      1. Cast directly to Date (works if already Date or ISO string)
      2. Parse as YYYYMMDD string
      3. Parse as YYYY-MM-DD string

    Returns:
        A Polars expression aliased to *col_name*.
    """
    return pl.coalesce([
        pl.col(col_name).cast(pl.Date, strict=False),
        pl.col(col_name).cast(pl.Utf8, strict=False).str.strptime(pl.Date, format="%Y%m%d", strict=False),
        pl.col(col_name).cast(pl.Utf8, strict=False).str.strptime(pl.Date, format="%Y-%m-%d", strict=False),
    ]).alias(col_name)
