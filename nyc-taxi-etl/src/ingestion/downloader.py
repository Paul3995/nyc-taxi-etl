"""
Ingestion layer: downloads raw NYC TLC Yellow Taxi data from the public website.

Design decision: stream with iter_content rather than loading the full file
into memory, so this works even when the Parquet file is several hundred MB.
Files are skipped if they already exist locally, making reruns idempotent.
"""
import logging
from pathlib import Path

import requests

from config.settings import (
    DATA_RAW_DIR,
    DEFAULT_MONTH,
    DEFAULT_YEAR,
    TLC_BASE_URL,
    TLC_ZONE_LOOKUP_URL,
)

logger = logging.getLogger(__name__)

_CHUNK_SIZE = 8_192  # bytes per read chunk


def download_file(url: str, dest: Path, chunk_size: int = _CHUNK_SIZE) -> Path:
    """
    Stream-download *url* to *dest*.

    Args:
        url: Remote URL to fetch.
        dest: Local path to write the file.
        chunk_size: Bytes read per iteration (keeps memory footprint flat).

    Returns:
        The resolved local path of the downloaded file.

    Raises:
        requests.HTTPError: If the server returns a non-2xx status.
    """
    if dest.exists():
        logger.info("Already on disk, skipping download: %s", dest.name)
        return dest

    dest.parent.mkdir(parents=True, exist_ok=True)
    logger.info("Downloading %s → %s", url, dest)

    with requests.get(url, stream=True, timeout=120) as resp:
        resp.raise_for_status()
        with dest.open("wb") as fh:
            for chunk in resp.iter_content(chunk_size=chunk_size):
                fh.write(chunk)

    size_mb = dest.stat().st_size / 1_048_576
    logger.info("Saved %s (%.1f MB)", dest.name, size_mb)
    return dest


def download_trip_data(year: int = DEFAULT_YEAR, month: int = DEFAULT_MONTH) -> Path:
    """
    Download the Yellow Taxi Parquet file for a given year/month.

    Args:
        year: Four-digit year (e.g. 2024).
        month: Month number 1–12.

    Returns:
        Local path of the downloaded Parquet file.
    """
    filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
    url = f"{TLC_BASE_URL}/{filename}"
    return download_file(url, DATA_RAW_DIR / filename)


def download_zone_lookup() -> Path:
    """
    Download the taxi zone lookup CSV.

    The lookup maps the integer LocationID fields in trip records to
    human-readable Borough and Zone names (≈265 rows, ~10 KB).

    Returns:
        Local path of the downloaded CSV.
    """
    dest = DATA_RAW_DIR / "taxi_zone_lookup.csv"
    return download_file(TLC_ZONE_LOOKUP_URL, dest)


def ingest(year: int = DEFAULT_YEAR, month: int = DEFAULT_MONTH) -> tuple[Path, Path]:
    """
    Run the full ingestion step: trip Parquet + zone lookup CSV.

    Args:
        year: Year of the trip data to ingest.
        month: Month of the trip data to ingest.

    Returns:
        Tuple of (trip_parquet_path, zone_lookup_path).
    """
    trip_path = download_trip_data(year, month)
    zone_path = download_zone_lookup()
    return trip_path, zone_path
