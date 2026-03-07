from __future__ import annotations

"""DuckDB helpers for efficient joins and aggregations."""

from contextlib import contextmanager
from pathlib import Path
from typing import Iterable, Mapping, Optional

import duckdb
import polars as pl

from src.utils.io import PipelineContext


@contextmanager
def duckdb_connection(ctx: PipelineContext, database: Optional[Path] = None):
    path = database or (ctx.config.paths.cache_root / "pipeline.duckdb")
    conn = duckdb.connect(str(path))
    try:
        yield conn
    finally:
        conn.close()


def execute_query(
    ctx: PipelineContext,
    sql: str,
    bindings: Optional[Mapping[str, object]] = None,
    *,
    database: Optional[Path] = None,
) -> pl.DataFrame:
    with duckdb_connection(ctx, database=database) as conn:
        result = conn.execute(sql, bindings)
        return pl.from_arrow(result.arrow())


def register_polars(conn: duckdb.DuckDBPyConnection, name: str, df: pl.DataFrame) -> None:
    conn.register(name, df.to_arrow())


def register_parquet(conn: duckdb.DuckDBPyConnection, name: str, paths: Iterable[Path | str]) -> None:
    conn.register(name, [str(Path(p)) for p in paths])


__all__ = ["duckdb_connection", "execute_query", "register_polars", "register_parquet"]
