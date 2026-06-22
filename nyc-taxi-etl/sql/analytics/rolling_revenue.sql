-- rolling_revenue.sql
--
-- Business question: Which zones are trending above or below their recent baseline?
--
-- This query surfaces the 7-day rolling average revenue that was computed in
-- PySpark (src/transformation/transforms.py :: add_rolling_revenue) and stored
-- in the daily_zone_stats view.  The pct_vs_rolling_avg column answers:
-- "Is today's revenue for this zone above or below its own recent trend?"
--
-- Use case: Early detection of demand shifts (events, weather, road closures).
-- A zone spiking 40 % above its rolling average on a Tuesday is a signal
-- worth investigating.

SELECT
    pickup_date,
    pickup_zone,
    pickup_borough,
    total_trips,
    total_revenue                                           AS daily_revenue,
    rolling_7d_avg_revenue,
    ROUND(
        (total_revenue - rolling_7d_avg_revenue)
        / NULLIF(rolling_7d_avg_revenue, 0) * 100,
    2)                                                      AS pct_vs_rolling_avg
FROM daily_zone_stats
WHERE pickup_zone IS NOT NULL
ORDER BY pickup_date DESC, total_revenue DESC;
