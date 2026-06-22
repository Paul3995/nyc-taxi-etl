"""
Pipeline orchestrator: runs ingestion → transformation → loading in sequence.

Usage (from project root):
    python -m src.pipeline
    python -m src.pipeline --year 2024 --month 3
"""
from __future__ import annotations

import argparse
import logging
import sys
import time

from config.settings import DATA_PROCESSED_DIR, DB_PATH, LOG_LEVEL
from src.ingestion.downloader import ingest
from src.loading.loader import load
from src.transformation.transforms import get_spark, run_transforms

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s  [%(levelname)-8s]  %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="NYC Taxi ETL Pipeline")
    parser.add_argument("--year", type=int, default=2024, help="TLC data year")
    parser.add_argument("--month", type=int, default=1, help="TLC data month (1–12)")
    return parser.parse_args(argv)


def main(year: int = 2024, month: int = 1) -> None:
    t0 = time.perf_counter()
    logger.info("=" * 55)
    logger.info("NYC Taxi ETL Pipeline  —  %d-%02d", year, month)
    logger.info("=" * 55)

    # ── Stage 1: Ingestion ──────────────────────────────────────
    logger.info("Stage 1/3 — Ingestion")
    trip_path, zone_path = ingest(year=year, month=month)

    # ── Stage 2: Transform ──────────────────────────────────────
    logger.info("Stage 2/3 — Transform (PySpark)")
    spark = get_spark()
    try:
        run_transforms(spark, trip_path, zone_path, DATA_PROCESSED_DIR)
    finally:
        spark.stop()

    # ── Stage 3: Load ───────────────────────────────────────────
    logger.info("Stage 3/3 — Load (DuckDB)")
    conn = load(DB_PATH, DATA_PROCESSED_DIR)
    conn.close()

    elapsed = time.perf_counter() - t0
    logger.info("=" * 55)
    logger.info("Pipeline complete in %.1f s", elapsed)
    logger.info("=" * 55)


if __name__ == "__main__":
    args = parse_args(sys.argv[1:])
    main(year=args.year, month=args.month)
