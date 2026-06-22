"""
Unit tests for PySpark transformation logic.

All tests create small in-memory DataFrames so they run without any files
on disk.  Each test class covers one transformation function and includes
both positive (keeps good data) and negative (drops/flags bad data) cases.
"""
from __future__ import annotations

import datetime

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql import types as T

from src.transformation.transforms import (
    add_derived_columns,
    add_rolling_revenue,
    clean_trips,
    compute_daily_zone_stats,
    join_with_zones,
)

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

TRIP_SCHEMA = T.StructType([
    T.StructField("tpep_pickup_datetime", T.TimestampType()),
    T.StructField("tpep_dropoff_datetime", T.TimestampType()),
    T.StructField("PULocationID", T.IntegerType()),
    T.StructField("DOLocationID", T.IntegerType()),
    T.StructField("trip_distance", T.DoubleType()),
    T.StructField("fare_amount", T.DoubleType()),
    T.StructField("tip_amount", T.DoubleType()),
    T.StructField("total_amount", T.DoubleType()),
    T.StructField("passenger_count", T.LongType()),
    T.StructField("payment_type", T.LongType()),
])

ZONE_SCHEMA = T.StructType([
    T.StructField("LocationID", T.IntegerType()),
    T.StructField("Borough", T.StringType()),
    T.StructField("Zone", T.StringType()),
    T.StructField("service_zone", T.StringType()),
])

# Two timestamps 30 minutes apart used across multiple tests
_PICKUP = datetime.datetime(2024, 1, 15, 9, 0, 0)
_DROPOFF = datetime.datetime(2024, 1, 15, 9, 30, 0)


def _make_trips(spark: SparkSession, rows: list[dict]):
    return spark.createDataFrame([Row(**r) for r in rows], schema=TRIP_SCHEMA)


def _valid_row(**overrides) -> dict:
    """Return a minimal valid trip row, allowing field overrides."""
    base = {
        "tpep_pickup_datetime": _PICKUP,
        "tpep_dropoff_datetime": _DROPOFF,
        "PULocationID": 1,
        "DOLocationID": 2,
        "trip_distance": 5.0,
        "fare_amount": 15.0,
        "tip_amount": 3.0,
        "total_amount": 18.0,
        "passenger_count": 2,
        "payment_type": 1,
    }
    return {**base, **overrides}


# ---------------------------------------------------------------------------
# clean_trips
# ---------------------------------------------------------------------------

class TestCleanTrips:
    def test_keeps_valid_row(self, spark):
        df = _make_trips(spark, [_valid_row()])
        assert clean_trips(df).count() == 1

    @pytest.mark.parametrize("distance", [0.0, 0.05, 250.0])
    def test_removes_out_of_range_distance(self, spark, distance):
        df = _make_trips(spark, [_valid_row(trip_distance=distance)])
        assert clean_trips(df).count() == 0

    @pytest.mark.parametrize("fare", [-1.0, 0.0, 600.0])
    def test_removes_invalid_fare(self, spark, fare):
        df = _make_trips(spark, [_valid_row(fare_amount=fare)])
        assert clean_trips(df).count() == 0

    @pytest.mark.parametrize("pax", [0, 7])
    def test_removes_invalid_passenger_count(self, spark, pax):
        df = _make_trips(spark, [_valid_row(passenger_count=pax)])
        assert clean_trips(df).count() == 0

    def test_removes_dropoff_before_pickup(self, spark):
        df = _make_trips(spark, [_valid_row(
            tpep_pickup_datetime=_DROPOFF,
            tpep_dropoff_datetime=_PICKUP,  # reversed
        )])
        assert clean_trips(df).count() == 0

    def test_removes_null_distance(self, spark):
        df = _make_trips(spark, [_valid_row(trip_distance=None)])
        assert clean_trips(df).count() == 0

    def test_keeps_multiple_valid_rows(self, spark):
        rows = [_valid_row(trip_distance=d) for d in [1.0, 2.5, 10.0]]
        df = _make_trips(spark, rows)
        assert clean_trips(df).count() == 3


# ---------------------------------------------------------------------------
# add_derived_columns
# ---------------------------------------------------------------------------

class TestAddDerivedColumns:
    def test_trip_duration_30_minutes(self, spark):
        df = _make_trips(spark, [_valid_row()])
        row = add_derived_columns(df).collect()[0]
        assert row["trip_duration_minutes"] == pytest.approx(30.0, abs=0.1)

    def test_speed_10_mph(self, spark):
        # 5 miles in 30 min = 10 mph
        df = _make_trips(spark, [_valid_row()])
        row = add_derived_columns(df).collect()[0]
        assert row["speed_mph"] == pytest.approx(10.0, abs=0.1)

    def test_tip_pct_20_percent(self, spark):
        # tip=3, fare=15 → 20 %
        df = _make_trips(spark, [_valid_row()])
        row = add_derived_columns(df).collect()[0]
        assert row["tip_pct"] == pytest.approx(20.0, abs=0.01)

    def test_tip_pct_zero_when_fare_is_zero(self, spark):
        df = _make_trips(spark, [_valid_row(fare_amount=0.01, tip_amount=0.0)])
        row = add_derived_columns(df).collect()[0]
        assert row["tip_pct"] == pytest.approx(0.0, abs=0.01)

    def test_pickup_date_extracted(self, spark):
        df = _make_trips(spark, [_valid_row()])
        row = add_derived_columns(df).collect()[0]
        assert row["pickup_date"] == datetime.date(2024, 1, 15)

    def test_pickup_hour_extracted(self, spark):
        df = _make_trips(spark, [_valid_row()])
        row = add_derived_columns(df).collect()[0]
        assert row["pickup_hour"] == 9

    def test_year_and_month_extracted(self, spark):
        df = _make_trips(spark, [_valid_row()])
        row = add_derived_columns(df).collect()[0]
        assert row["pickup_year"] == 2024
        assert row["pickup_month"] == 1


# ---------------------------------------------------------------------------
# join_with_zones
# ---------------------------------------------------------------------------

class TestJoinWithZones:
    def _make_zones(self, spark):
        rows = [
            Row(LocationID=1, Borough="Manhattan", Zone="Midtown Center", service_zone="Yellow Zone"),
            Row(LocationID=2, Borough="Brooklyn", Zone="Park Slope", service_zone="Yellow Zone"),
        ]
        return spark.createDataFrame(rows, schema=ZONE_SCHEMA)

    def test_pickup_zone_attached(self, spark):
        trips = _make_trips(spark, [_valid_row(PULocationID=1)])
        result = join_with_zones(trips, self._make_zones(spark)).collect()[0]
        assert result["pickup_zone"] == "Midtown Center"
        assert result["pickup_borough"] == "Manhattan"

    def test_dropoff_zone_attached(self, spark):
        trips = _make_trips(spark, [_valid_row(DOLocationID=2)])
        result = join_with_zones(trips, self._make_zones(spark)).collect()[0]
        assert result["dropoff_zone"] == "Park Slope"
        assert result["dropoff_borough"] == "Brooklyn"

    def test_unknown_location_gives_null(self, spark):
        trips = _make_trips(spark, [_valid_row(PULocationID=999)])
        result = join_with_zones(trips, self._make_zones(spark)).collect()[0]
        assert result["pickup_zone"] is None

    def test_row_count_unchanged(self, spark):
        rows = [_valid_row(PULocationID=1), _valid_row(PULocationID=2)]
        trips = _make_trips(spark, rows)
        result = join_with_zones(trips, self._make_zones(spark))
        assert result.count() == 2


# ---------------------------------------------------------------------------
# compute_daily_zone_stats
# ---------------------------------------------------------------------------

class TestComputeDailyZoneStats:
    def _enriched_df(self, spark):
        """Build a minimal enriched trips DataFrame with zone columns."""
        schema = T.StructType([
            T.StructField("pickup_date", T.DateType()),
            T.StructField("PULocationID", T.IntegerType()),
            T.StructField("pickup_zone", T.StringType()),
            T.StructField("pickup_borough", T.StringType()),
            T.StructField("total_amount", T.DoubleType()),
            T.StructField("trip_distance", T.DoubleType()),
            T.StructField("trip_duration_minutes", T.DoubleType()),
            T.StructField("tip_pct", T.DoubleType()),
            T.StructField("passenger_count", T.LongType()),
        ])
        rows = [
            Row(
                pickup_date=datetime.date(2024, 1, 15),
                PULocationID=1,
                pickup_zone="Midtown",
                pickup_borough="Manhattan",
                total_amount=20.0,
                trip_distance=3.0,
                trip_duration_minutes=15.0,
                tip_pct=18.0,
                passenger_count=1,
            ),
            Row(
                pickup_date=datetime.date(2024, 1, 15),
                PULocationID=1,
                pickup_zone="Midtown",
                pickup_borough="Manhattan",
                total_amount=30.0,
                trip_distance=5.0,
                trip_duration_minutes=25.0,
                tip_pct=22.0,
                passenger_count=2,
            ),
        ]
        return spark.createDataFrame(rows, schema=schema)

    def test_aggregates_to_one_row(self, spark):
        result = compute_daily_zone_stats(self._enriched_df(spark))
        assert result.count() == 1

    def test_total_revenue(self, spark):
        row = compute_daily_zone_stats(self._enriched_df(spark)).collect()[0]
        assert row["total_revenue"] == pytest.approx(50.0, abs=0.01)

    def test_total_trips(self, spark):
        row = compute_daily_zone_stats(self._enriched_df(spark)).collect()[0]
        assert row["total_trips"] == 2

    def test_avg_tip_pct(self, spark):
        row = compute_daily_zone_stats(self._enriched_df(spark)).collect()[0]
        assert row["avg_tip_pct"] == pytest.approx(20.0, abs=0.01)


# ---------------------------------------------------------------------------
# add_rolling_revenue
# ---------------------------------------------------------------------------

class TestAddRollingRevenue:
    def _daily_stats_df(self, spark):
        """Three days of stats for one zone."""
        schema = T.StructType([
            T.StructField("pickup_date", T.DateType()),
            T.StructField("PULocationID", T.IntegerType()),
            T.StructField("pickup_zone", T.StringType()),
            T.StructField("pickup_borough", T.StringType()),
            T.StructField("total_trips", T.LongType()),
            T.StructField("total_revenue", T.DoubleType()),
            T.StructField("avg_distance_miles", T.DoubleType()),
            T.StructField("avg_duration_minutes", T.DoubleType()),
            T.StructField("avg_tip_pct", T.DoubleType()),
            T.StructField("total_passengers", T.LongType()),
        ])
        rows = [
            Row(pickup_date=datetime.date(2024, 1, d), PULocationID=1,
                pickup_zone="Z", pickup_borough="B",
                total_trips=100, total_revenue=float(1000 * d),
                avg_distance_miles=3.0, avg_duration_minutes=15.0,
                avg_tip_pct=18.0, total_passengers=120)
            for d in range(1, 4)
        ]
        return spark.createDataFrame(rows, schema=schema)

    def test_rolling_column_exists(self, spark):
        result = add_rolling_revenue(self._daily_stats_df(spark))
        assert "rolling_7d_avg_revenue" in result.columns

    def test_rolling_avg_on_day3(self, spark):
        # Day 1: 1000, Day 2: 2000, Day 3: 3000 → avg = 2000
        rows = (
            add_rolling_revenue(self._daily_stats_df(spark))
            .orderBy("pickup_date")
            .collect()
        )
        assert rows[2]["rolling_7d_avg_revenue"] == pytest.approx(2000.0, abs=1.0)
