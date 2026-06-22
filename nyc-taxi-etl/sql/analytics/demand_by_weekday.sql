-- demand_by_weekday.sql
--
-- Business question: How does demand differ across days of the week?
--
-- Use case: Weekly seasonality modelling, driver availability planning.
-- avg_daily_trips normalises for the fact that a month may contain 4 Mondays
-- but 5 Fridays, preventing a raw trip count from misleading the analysis.

SELECT
    DAYNAME(pickup_date)                                    AS day_of_week,
    DAYOFWEEK(pickup_date)                                  AS day_num,       -- 1=Sun, 7=Sat
    COUNT(*)                                                AS total_trips,
    ROUND(SUM(total_amount), 2)                             AS total_revenue,
    ROUND(AVG(total_amount), 2)                             AS avg_fare,
    ROUND(AVG(trip_distance), 2)                            AS avg_distance_miles,
    COUNT(DISTINCT pickup_date)                             AS num_days_in_sample,
    ROUND(COUNT(*) / COUNT(DISTINCT pickup_date), 0)        AS avg_daily_trips
FROM trips
GROUP BY day_of_week, day_num
ORDER BY day_num;
