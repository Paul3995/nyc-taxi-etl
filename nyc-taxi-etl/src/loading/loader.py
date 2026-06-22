"""
Loading layer: registers processed Parquet files as DuckDB views.

Design decision: we use CREATE OR REPLACE VIEW (backed by read_parquet) rather
than COPY INTO a materialised table.  This means:
  - No data is duplicated on disk — the Parquet files are the source of truth.
  - DuckDB still uses its vectorised Parquet reader and hive partition pruning.
  - Re-running the loader is fully idempotent (no DROP/CREATE dance needed).

If query performance became a bottleneck, switching to materialised tables
would be a one-line change per view.
"""
from __future__ import annotations

import logging
from pathlib import Path

import duckdb

from config.settings import DATA_PROCESSED_DIR, DB_PATH

logger = logging.getLogger(__name__)


def get_connection(db_path: Path = DB_PATH) -> duckdb.DuckDBPyConnection:
    """
    Open (or create) the DuckDB database file.

    Args:
        db_path: Filesystem path for the .duckdb file.

    Returns:
        An open DuckDB connection.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))
    logger.info("Connected to DuckDB at %s", db_path)
    return conn


def register_views(
    conn: duckdb.DuckDBPyConnection,
    processed_dir: Path = DATA_PROCESSED_DIR,
) -> None:
    """
    Register Parquet directories as DuckDB views.

    hive_partitioning=true tells DuckDB to interpret pickup_year=.../
    pickup_month=.../ folder names as virtual columns, so queries like
    WHERE pickup_year = 2024 AND pickup_month = 1 skip irrelevant files.

    Args:
        conn: Open DuckDB connection.
        processed_dir: Root directory containing trips/ and daily_zone_stats/.
    """
    trips_glob = str(processed_dir / "trips" / "**" / "*.parquet")
    stats_glob = str(processed_dir / "daily_zone_stats" / "*.parquet")

    conn.execute(f"""
        CREATE OR REPLACE VIEW trips AS
        SELECT * FROM read_parquet('{trips_glob}', hive_partitioning = true)
    """)
    logger.info("Registered view: trips  (%s)", trips_glob)

    conn.execute(f"""
        CREATE OR REPLACE VIEW daily_zone_stats AS
        SELECT * FROM read_parquet('{stats_glob}')
    """)
    logger.info("Registered view: daily_zone_stats  (%s)", stats_glob)


def preview(conn: duckdb.DuckDBPyConnection) -> None:
    """Log row counts for each registered view as a quick sanity check."""
    for view in ("trips", "daily_zone_stats"):
        count = conn.execute(f"SELECT COUNT(*) FROM {view}").fetchone()[0]  # type: ignore[index]
        logger.info("View %-20s → %s rows", view, f"{count:,}")


def load(
    db_path: Path = DB_PATH,
    processed_dir: Path = DATA_PROCESSED_DIR,
) -> duckdb.DuckDBPyConnection:
    """
    Full load step: open the database and register all views.

    Args:
        db_path: Path to the DuckDB file.
        processed_dir: Root directory of processed Parquet files.

    Returns:
        Open DuckDB connection (caller is responsible for closing).
    """
    conn = get_connection(db_path)
    register_views(conn, processed_dir)
    preview(conn)
    return conn
