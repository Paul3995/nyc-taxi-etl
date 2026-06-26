# NYC Taxi ETL Pipeline

A production-style, end-to-end data engineering pipeline built on **Python**, **PySpark**, and **DuckDB**.  
Ingests NYC Yellow Taxi trip data from the public TLC dataset, runs it through a multi-stage PySpark transformation layer, loads it into a local DuckDB database, and exposes a SQL analytics layer that answers real business questions.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          ETL Pipeline                                   │
│                                                                         │
│  ┌──────────────┐    ┌────────────────────┐    ┌──────────┐            │
│  │   Ingestion  │    │  PySpark Transform  │    │  Loader  │            │
│  │  (Python)    │───▶│                    │───▶│ (Python) │            │
│  │              │    │  • Schema enforce  │    │          │            │
│  │  Download    │    │  • Clean + filter  │    │  DuckDB  │            │
│  │  Yellow Taxi │    │  • Derived columns │    │  views   │            │
│  │  Parquet +   │    │  • Broadcast join  │    │          │            │
│  │  Zone CSV    │    │  • Aggregations    │    └────┬─────┘            │
│  │  from TLC    │    │  • Window function │         │                  │
│  └──────┬───────┘    └────────┬───────────┘         │                  │
│         │                     │                      │                  │
│    data/raw/           data/processed/          taxi.duckdb            │
│    (gitignored)        (gitignored)                  │                  │
│                                                      ▼                  │
│                                             ┌────────────────┐          │
│                                             │  SQL Analytics │          │
│                                             │                │          │
│                                             │  revenue_by_   │          │
│                                             │  hour.sql      │          │
│                                             │  top_pickup_   │          │
│                                             │  zones.sql     │          │
│                                             │  tip_rate_     │          │
│                                             │  analysis.sql  │          │
│                                             │  demand_by_    │          │
│                                             │  weekday.sql   │          │
│                                             │  rolling_      │          │
│                                             │  revenue.sql   │          │
│                                             └────────────────┘          │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology | Why |
|---|---|---|
| Ingestion | Python + `requests` | Lightweight; streaming download keeps memory flat |
| Transformation | PySpark 3.5 | Distributed processing; demonstrates partitioning, joins, window functions |
| Storage | Apache Parquet | Columnar, compressed, partition-prunable |
| Analytics DB | DuckDB | Zero-config, full SQL, vectorised Parquet reader — no server needed |
| Config | `python-dotenv` | Secrets stay in `.env`, never in source control |
| Testing | `pytest` | Session-scoped SparkSession keeps the suite fast |
| CI | GitHub Actions | Runs on every push; Java + Python setup automated |

---

## Project Structure

```
nyc-taxi-etl/
│
├── .github/workflows/ci.yml       # GitHub Actions CI
│
├── src/
│   ├── ingestion/downloader.py    # Download trip Parquet + zone CSV
│   ├── transformation/transforms.py  # All PySpark logic
│   ├── loading/loader.py          # Register Parquet as DuckDB views
│   └── pipeline.py                # Orchestrator (runs all three stages)
│
├── sql/analytics/
│   ├── revenue_by_hour.sql        # Peak revenue windows
│   ├── top_pickup_zones.sql       # Highest-revenue pickup areas
│   ├── tip_rate_analysis.sql      # Tip behaviour by payment + distance
│   ├── demand_by_weekday.sql      # Weekly seasonality
│   └── rolling_revenue.sql        # 7-day rolling avg per zone
│
├── tests/
│   ├── conftest.py                # Session-scoped SparkSession fixture
│   └── test_transforms.py         # ~25 pytest cases for each transform
│
├── config/settings.py             # Central config; reads .env
├── data/raw/                      # Gitignored — downloaded data lands here
├── data/processed/                # Gitignored — Spark output lands here
├── .env.example                   # Template for local config
├── .gitignore
├── requirements.txt
└── README.md
```

---

## How to Run Locally

### Prerequisites

- Python 3.11+
- Java 11+ (required by PySpark — check with `java -version`)

### 1. Clone and install

```bash
git clone https://github.com/Paul3995/nyc-taxi-etl.git
cd nyc-taxi-etl
python -m venv .venv
source .venv/bin/activate       # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure

```bash
cp .env.example .env
# Edit .env if you want a different month (default: January 2024)
```

### 3. Run the pipeline

```bash
python -m src.pipeline
# Or choose a different month:
python -m src.pipeline --year 2024 --month 3
```

The pipeline will:
1. Download `yellow_tripdata_2024-01.parquet` (~50 MB) and `taxi_zone_lookup.csv`
2. Run PySpark transformations (clean → enrich → join → aggregate → window)
3. Write partitioned Parquet to `data/processed/`
4. Register DuckDB views and log row counts

### 4. Query the data

```bash
python - <<'EOF'
import duckdb
conn = duckdb.connect("taxi.duckdb")

# Top 10 revenue zones
print(conn.execute(open("sql/analytics/top_pickup_zones.sql").read()).df().head(10))

# Hourly demand
print(conn.execute(open("sql/analytics/revenue_by_hour.sql").read()).df())
EOF
```

### 5. Run the tests

```bash
pytest tests/ -v --tb=short
```

---

## Sample Output

**Top pickup zones by revenue (Jan 2024)**

| pickup_zone | pickup_borough | total_trips | total_revenue | avg_fare |
|---|---|---|---|---|
| JFK Airport | Queens | 38 421 | $1 284 765 | $33.44 |
| Midtown Center | Manhattan | 91 203 | $1 101 442 | $12.07 |
| Penn Station/Madison Sq W | Manhattan | 67 891 | $897 234 | $13.21 |
| Upper East Side North | Manhattan | 82 014 | $861 903 | $10.51 |
| Times Sq/Theatre District | Manhattan | 74 329 | $819 022 | $11.02 |

**Revenue by hour of day**

| hour_of_day | total_trips | total_revenue | avg_fare | revenue_per_trip |
|---|---|---|---|---|
| 0 | 28 441 | $521 302 | $18.33 | $18.33 |
| 8 | 112 334 | $1 589 441 | $14.15 | $14.15 |
| 18 | 189 221 | $2 341 882 | $12.38 | $12.38 |
| 22 | 134 002 | $1 908 234 | $14.24 | $14.24 |

---

## Design Decisions & What I Learned

### PySpark choices

**Shuffle partitions set to 8 (not the default 200)**  
Spark's default of 200 shuffle partitions is tuned for large clusters.  
On a single machine processing ~3 M rows, 200 tiny tasks add scheduler overhead
with no benefit.  Setting it to 8 (≈ 2× CPU cores) keeps tasks large enough
to be worth the fork cost.

**Broadcast join for zone lookup**  
The zone lookup table has 265 rows.  Without a broadcast hint, Spark would
sort-merge join two DataFrames — shuffling the 3 M-row trips table across the
network (even locally, this means writing shuffle files).  With `F.broadcast()`,
the small table is copied to every executor and the trips side never moves.

**`rangeBetween` vs `rowsBetween` in the window function**  
`rowsBetween(-6, 0)` counts 6 preceding *rows*, which breaks silently when
a zone has missing days (e.g., no trips on Christmas).  `rangeBetween` works
on the numeric value of the ORDER BY column (Unix timestamp in seconds), so it
always spans exactly 7 calendar days regardless of gaps.

**Partition by year/month at write time**  
Downstream consumers (DuckDB, Athena, a future Spark job) can push predicates
like `WHERE pickup_year = 2024 AND pickup_month = 1` all the way to the file
scanner, skipping entire directories.  For a multi-year dataset this is the
single biggest query-latency win available at the storage layer.

### Database choices

**DuckDB over Postgres/SQLite**  
SQLite has no parallel query execution.  Postgres needs a running server
(defeats "runs on a single machine" requirement).  DuckDB is columnar,
vectorised, and reads Parquet directly without an import step — it is the
right tool for this analytical workload.

**Views over materialised tables**  
`CREATE VIEW` backed by `read_parquet()` means the Parquet files are the
single source of truth with no data duplication.  If the pipeline reruns,
the views automatically reflect the new data without a DROP/truncate/reload
cycle.  A materialised table would only be warranted if query latency became
a problem at much larger data volumes.

### Config & secrets

All tuneable values (paths, year, month, log level) live in `config/settings.py`
and are read from environment variables or a `.env` file.  The `.env` file is
gitignored; `.env.example` documents every variable so a new contributor can
be up and running with one `cp` command.  No secrets appear anywhere in source.

### Testing strategy

Each transformation function is tested in isolation with small in-memory
DataFrames, so tests run in seconds and never touch the filesystem.  The
SparkSession is `session`-scoped in `conftest.py` — it starts once for the
entire suite (~8–10 s) rather than once per test.  Parametrize is used on
filter tests to cover multiple bad-data variants without repeating boilerplate.
