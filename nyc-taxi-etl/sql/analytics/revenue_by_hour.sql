-- revenue_by_hour.sql
--
-- Business question: When during the day does the most revenue flow?
--
-- Use case: Surge-pricing windows, driver shift scheduling, demand forecasting.
-- The revenue_per_trip column normalises by trip count so a high-volume
-- midnight hour isn't confused with a genuinely high-value hour.

SELECT
    pickup_hour                                             AS hour_of_day,
    COUNT(*)                                                AS total_trips,
    ROUND(SUM(total_amount), 2)                             AS total_revenue,
    ROUND(AVG(total_amount), 2)                             AS avg_fare,
    ROUND(AVG(trip_distance), 2)                            AS avg_distance_miles,
    ROUND(SUM(total_amount) / COUNT(*), 2)                  AS revenue_per_trip
FROM trips
GROUP BY pickup_hour
ORDER BY pickup_hour;
