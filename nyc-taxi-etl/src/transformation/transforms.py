"""
PySpark transformation layer for NYC Yellow Taxi trip data.

Each public function is a pure transformation: DataFrame in, DataFrame out,
no side effects.  This makes them easy to unit-test with small in-memory
DataFrames without touching the filesystem.

Design decisions are commented inline so they are defensible in code review
and interviews.
"""
from __future__ import annotations

import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

from config.settings import DATA_PROCESSED_DIR

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema definitions
# ---------------------------------------------------------------------------
# Enforcing schema on read (rather than relying on Parquet's embedded schema)
# makes the pipeline fail fast with a clear error if the TLC source format
# changes — far better than silent bad data flowing downstream.

TRIP_SCHEMA = T.StructType([
    T.StructField("VendorID", T.IntegerType(), True),
    T.StructField("tpep_pickup_datetime", T.TimestampType(), True),
    T.StructField("tpep_dropoff_datetime", T.TimestampType(), True),
    T.StructField("passenger_count", T.LongType(), True),
    T.StructField("trip_distance", T.DoubleType(), True),
    T.StructField("RatecodeID", T.LongType(), True),
    T.StructField("store_and_fwd_flag", T.StringType(), True),
    T.StructField("PULocationID", T.IntegerType(), True),
    T.StructField("DOLocationID", T.IntegerType(), True),
    T.StructField("payment_type", T.LongType(), True),
    T.StructField("fare_amount", T.DoubleType(), True),
    T.StructField("extra", T.DoubleType(), True),
    T.StructField("mta_tax", T.DoubleType(), True),
    T.StructField("tip_amount", T.DoubleType(), True),
    T.StructField("tolls_amount", T.DoubleType(), True),
    T.StructField("improvement_surcharge", T.DoubleType(), True),
    T.StructField("total_amount", T.DoubleType(), True),
    T.StructField("congestion_surcharge", T.DoubleType(), True),
    T.StructField("airport_fee", T.DoubleType(), True),
])

ZONE_SCHEMA = T.StructType([
    T.StructField("LocationID", T.IntegerType(), True),
    T.StructField("Borough", T.StringType(), True),
    T.StructField("Zone", T.StringType(), True),
    T.StructField("service_zone", T.StringType(), True),
])


# ---------------------------------------------------------------------------
# Session
# ---------------------------------------------------------------------------

def get_spark(app_name: str = "nyc-taxi-etl") -> SparkSession:
    """
    Create (or reuse) a local SparkSession.

    spark.sql.shuffle.partitions defaults to 200, which is tuned for large
    clusters.  For single-machine work on ~3 M rows we drop it to 8 to avoid
    creating 200 tiny shuffle files and wasting task-scheduling overhead.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")  # use all available cores; swap for cluster URL in prod
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Readers
# ---------------------------------------------------------------------------

def read_trips(spark: SparkSession, path: Path) -> DataFrame:
    """
    Read raw Yellow Taxi Parquet with schema enforcement.

    Args:
        spark: Active SparkSession.
        path: Local path to the Parquet file.

    Returns:
        Raw trips DataFrame.
    """
    return spark.read.schema(TRIP_SCHEMA).parquet(str(path))


def read_zones(spark: SparkSession, path: Path) -> DataFrame:
    """
    Read the taxi zone lookup CSV.

    Args:
        spark: Active SparkSession.
        path: Local path to taxi_zone_lookup.csv.

    Returns:
        Zone lookup DataFrame (LocationID, Borough, Zone, service_zone).
    """
    return (
        spark.read
        .schema(ZONE_SCHEMA)
        .option("header", "true")
        .csv(str(path))
    )


# ---------------------------------------------------------------------------
# Transformations
# ---------------------------------------------------------------------------

def clean_trips(df: DataFrame) -> DataFrame:
    """
    Drop null critical fields and filter out business-logic outliers.

    Outlier bounds are derived from the TLC data dictionary and exploratory
    analysis of the January 2024 dataset:
    - Distance < 0.1 mi: GPS noise / cancelled trips
    - Distance > 200 mi: data entry errors (NYC metro area)
    - Fare ≤ 0: refunds or errors
    - Fare > 500: almost certainly an error for a city taxi
    - Passengers < 1: meter ran with no rider
    - Passengers > 6: exceeds TLC vehicle capacity limit
    - Dropoff before pickup: timestamp corruption

    Args:
        df: Raw trips DataFrame.

    Returns:
        Cleaned DataFrame with outliers removed.
    """
    critical_cols = [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "PULocationID",
        "DOLocationID",
        "trip_distance",
        "fare_amount",
        "total_amount",
    ]
    return (
        df
        .dropna(subset=critical_cols)
        .filter(
            (F.col("trip_distance") > 0.1) & (F.col("trip_distance") < 200)
            & (F.col("fare_amount") > 0) & (F.col("fare_amount") < 500)
            & (F.col("passenger_count") >= 1) & (F.col("passenger_count") <= 6)
            & (F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
        )
    )


def add_derived_columns(df: DataFrame) -> DataFrame:
    """
    Enrich each trip row with computed business metrics.

    Columns added:
    - trip_duration_minutes: float, used for speed & experience analysis
    - speed_mph: float, useful for congestion and routing studies
    - tip_pct: float, tip as % of base fare (excludes surcharges)
    - pickup_date: date, used for time-series aggregations
    - pickup_hour: int, used for hourly demand analysis
    - pickup_year / pickup_month: int, used for Parquet partitioning

    speed_mph guards against division by zero with a WHEN/NULL pattern.
    tip_pct treats $0 base fares as 0% to avoid division by zero.

    Args:
        df: Cleaned trips DataFrame.

    Returns:
        Enriched DataFrame.
    """
    duration_minutes = (
        F.col("tpep_dropoff_datetime").cast("long")
        - F.col("tpep_pickup_datetime").cast("long")
    ) / 60.0

    return (
        df
        .withColumn("trip_duration_minutes", duration_minutes)
        .withColumn(
            "speed_mph",
            F.when(
                duration_minutes > 0,
                F.col("trip_distance") / (duration_minutes / 60.0),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "tip_pct",
            F.when(
                F.col("fare_amount") > 0,
                F.col("tip_amount") / F.col("fare_amount") * 100,
            ).otherwise(F.lit(0.0)),
        )
        .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        .withColumn("pickup_year", F.year("tpep_pickup_datetime"))
        .withColumn("pickup_month", F.month("tpep_pickup_datetime"))
    )


def join_with_zones(trips_df: DataFrame, zones_df: DataFrame) -> DataFrame:
    """
    Enrich trips with human-readable pickup and dropoff zone names.

    broadcast() is explicitly applied to the small zones DataFrame (~265 rows)
    so Spark uses a broadcast hash join instead of a sort-merge join.
    This avoids a shuffle on the multi-million-row trips side, which would
    be the single most expensive operation in the pipeline.

    We alias and join twice (pickup + dropoff) to carry both zone names forward.

    Args:
        trips_df: Enriched trips DataFrame (must have PULocationID, DOLocationID).
        zones_df: Zone lookup DataFrame (must have LocationID, Borough, Zone).

    Returns:
        Trips DataFrame with pickup_zone, pickup_borough, dropoff_zone,
        dropoff_borough columns appended.
    """
    pickup_zones = zones_df.select(
        F.col("LocationID").alias("PULocationID"),
        F.col("Zone").alias("pickup_zone"),
        F.col("Borough").alias("pickup_borough"),
    )
    dropoff_zones = zones_df.select(
        F.col("LocationID").alias("DOLocationID"),
        F.col("Zone").alias("dropoff_zone"),
        F.col("Borough").alias("dropoff_borough"),
    )
    return (
        trips_df
        .join(F.broadcast(pickup_zones), on="PULocationID", how="left")
        .join(F.broadcast(dropoff_zones), on="DOLocationID", how="left")
    )


def compute_daily_zone_stats(df: DataFrame) -> DataFrame:
    """
    Aggregate trip-level data to daily × pickup zone granularity.

    This intermediate aggregation is the input to the rolling-average window
    function below.  Computing rolling stats on the raw 3 M-row trip table
    would force Spark to sort all rows by date per zone — extremely expensive.
    Pre-aggregating to ~(31 days × 265 zones) ≈ 8 k rows makes the window
    computation trivial.

    Args:
        df: Enriched and zone-joined trips DataFrame.

    Returns:
        Daily zone stats DataFrame.
    """
    return (
        df
        .groupBy("pickup_date", "PULocationID", "pickup_zone", "pickup_borough")
        .agg(
            F.count("*").alias("total_trips"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("trip_distance"), 3).alias("avg_distance_miles"),
            F.round(F.avg("trip_duration_minutes"), 2).alias("avg_duration_minutes"),
            F.round(F.avg("tip_pct"), 2).alias("avg_tip_pct"),
            F.sum("passenger_count").alias("total_passengers"),
        )
    )


def add_rolling_revenue(df: DataFrame) -> DataFrame:
    """
    Compute a 7-day rolling average revenue per pickup zone.

    Window spec rationale:
    - partitionBy(PULocationID): each zone gets its own independent window,
      so a slow week in Brooklyn doesn't dilute a busy week in Midtown.
    - orderBy(pickup_date cast to long): rangeBetween requires a numeric axis,
      so we convert the date to a Unix timestamp (seconds).
    - rangeBetween(-6 * 86400, 0): include today and the 6 preceding calendar
      days.  We prefer rangeBetween over rowsBetween(-6, 0) because
      rowsBetween counts rows (which breaks when data has missing days).

    Args:
        df: Daily zone stats DataFrame.

    Returns:
        Same DataFrame with rolling_7d_avg_revenue column appended.
    """
    seconds_per_day = 86_400
    window = (
        Window
        .partitionBy("PULocationID")
        .orderBy(F.col("pickup_date").cast("long"))
        .rangeBetween(-6 * seconds_per_day, 0)
    )
    return df.withColumn(
        "rolling_7d_avg_revenue",
        F.round(F.avg("total_revenue").over(window), 2),
    )


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------

def write_processed_trips(df: DataFrame, output_path: Path) -> None:
    """
    Write the enriched trip-level DataFrame as Parquet, partitioned by year/month.

    Partitioning by year and month lets any downstream consumer (DuckDB, Spark,
    Athena) prune partitions and skip files outside the queried time range.
    snappy compression is Parquet's default: fast decompression, reasonable ratio.

    Args:
        df: Enriched trips DataFrame (must have pickup_year, pickup_month).
        output_path: Root directory to write partitioned Parquet.
    """
    dest = str(output_path / "trips")
    (
        df
        .write
        .mode("overwrite")
        .partitionBy("pickup_year", "pickup_month")
        .parquet(dest)
    )
    logger.info("Wrote partitioned trips → %s", dest)


def write_daily_stats(df: DataFrame, output_path: Path) -> None:
    """
    Write the daily zone stats DataFrame as Parquet.

    Args:
        df: Daily zone stats with rolling_7d_avg_revenue.
        output_path: Root directory; data lands in output_path/daily_zone_stats/.
    """
    dest = str(output_path / "daily_zone_stats")
    df.write.mode("overwrite").parquet(dest)
    logger.info("Wrote daily zone stats → %s", dest)


# ---------------------------------------------------------------------------
# Pipeline entry point
# ---------------------------------------------------------------------------

def run_transforms(
    spark: SparkSession,
    trip_path: Path,
    zone_path: Path,
    output_path: Path = DATA_PROCESSED_DIR,
) -> None:
    """
    Execute the full transformation pipeline end-to-end.

    Stages:
        1. Read raw data
        2. Clean & validate
        3. Enrich with derived columns
        4. Join zone lookup
        5. Write partitioned trip-level Parquet
        6. Aggregate to daily zone stats
        7. Add rolling window metrics
        8. Write stats Parquet

    Args:
        spark: Active SparkSession.
        trip_path: Path to raw Yellow Taxi Parquet file.
        zone_path: Path to taxi_zone_lookup.csv.
        output_path: Root directory for processed output.
    """
    logger.info("Reading raw trips and zone lookup...")
    trips = read_trips(spark, trip_path)
    zones = read_zones(spark, zone_path)

    logger.info("Cleaning trips (null drops + outlier filters)...")
    trips = clean_trips(trips)

    logger.info("Adding derived columns...")
    trips = add_derived_columns(trips)

    logger.info("Joining with zone lookup (broadcast join)...")
    trips = join_with_zones(trips, zones)

    logger.info("Writing partitioned trip-level Parquet...")
    write_processed_trips(trips, output_path)

    logger.info("Computing daily zone aggregations...")
    daily_stats = compute_daily_zone_stats(trips)

    logger.info("Adding 7-day rolling revenue window...")
    daily_stats = add_rolling_revenue(daily_stats)

    logger.info("Writing daily zone stats...")
    write_daily_stats(daily_stats, output_path)

    logger.info("Transformation pipeline complete.")
