"""
Central configuration module.

All tuneable values live here so no magic strings are scattered across
the codebase.  Values are read from environment variables (or a .env file),
with sensible defaults for local development.  Secrets (if any were added)
never appear in source — only in .env, which is gitignored.
"""
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent

DATA_RAW_DIR = Path(os.getenv("DATA_RAW_DIR", str(BASE_DIR / "data" / "raw")))
DATA_PROCESSED_DIR = Path(os.getenv("DATA_PROCESSED_DIR", str(BASE_DIR / "data" / "processed")))
DB_PATH = Path(os.getenv("DB_PATH", str(BASE_DIR / "taxi.duckdb")))

LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

# NYC TLC public data endpoints — no auth required
TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
TLC_ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

DEFAULT_YEAR: int = int(os.getenv("TLC_YEAR", "2024"))
DEFAULT_MONTH: int = int(os.getenv("TLC_MONTH", "1"))
