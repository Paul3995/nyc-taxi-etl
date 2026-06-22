-- top_pickup_zones.sql
--
-- Business question: Which pickup zones generate the most revenue?
--
-- Use case: Fleet allocation, driver incentive zone bonuses, marketing.
-- trips_per_revenue_dollar inverts the question — a zone with many cheap
-- short trips has a worse ratio than one with fewer but longer airport runs.

SELECT
    pickup_zone,
    pickup_borough,
    COUNT(*)                                                AS total_trips,
    ROUND(SUM(total_amount), 2)                             AS total_revenue,
    ROUND(AVG(total_amount), 2)                             AS avg_fare,
    ROUND(AVG(trip_distance), 2)                            AS avg_distance_miles,
    ROUND(AVG(trip_duration_minutes), 1)                    AS avg_duration_minutes,
    ROUND(COUNT(*) / SUM(total_amount), 4)                  AS trips_per_revenue_dollar
FROM trips
WHERE pickup_zone IS NOT NULL
GROUP BY pickup_zone, pickup_borough
ORDER BY total_revenue DESC
LIMIT 20;
