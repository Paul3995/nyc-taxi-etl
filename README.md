# NYC Taxi ETL Pipeline

An end-to-end data engineering pipeline that ingests NYC Yellow Taxi trip data, transforms it with PySpark, loads it into DuckDB, and exposes a SQL analytics layer to answer real business questions about demand, revenue, and tipping behaviour.

## What It Does

The pipeline pulls Yellow Taxi trip records from the TLC public dataset (Parquet format) alongside a zone lookup CSV. It runs the raw data through a PySpark transformation layer — cleaning, filtering, deriving columns, and joining zone data via broadcast join — then loads the processed output into a local DuckDB database. From there, a set of SQL queries handle the analytics side.

The full architecture diagram and setup details live in [`nyc-taxi-etl/README.md`](nyc-taxi-etl/README.md).

## Tech Stack

- **Ingestion** — Python
- **Transformation** — PySpark 3.x
- **Storage & Analytics** — DuckDB
- **Testing** — pytest
- **Config** — python-dotenv

## Quick Start

```bash
git clone https://github.com/Paul3995/nyc-taxi-etl.git
cd nyc-taxi-etl/nyc-taxi-etl
pip install -r requirements.txt
cp .env.example .env
python -m src.pipeline
```

## SQL Queries Included

- `revenue_by_hour.sql` — when does revenue peak during the day?
- `top_pickup_zones.sql` — which zones are busiest?
- `tip_rate_analysis.sql` — how does tipping vary by zone and time?
- `demand_by_weekday.sql` — day-of-week demand patterns
- `rolling_revenue.sql` — 7-day rolling revenue trends

---
**Data Source:** NYC TLC Yellow Taxi Trip Records (public dataset)
