-- tip_rate_analysis.sql
--
-- Business question: How do tips vary by payment method and trip length?
--
-- Use case: Driver earnings forecasting; payment-method incentive design.
--
-- Note on cash trips (payment_type = 2): drivers receive cash tips off-system,
-- so tip_amount is always $0 in the data for cash payers.  We keep them in
-- the result but their avg_tip_pct will read 0 — a known data artefact, not
-- an indication that cash riders don't tip.

SELECT
    CASE payment_type
        WHEN 1 THEN 'Credit Card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No Charge'
        WHEN 4 THEN 'Dispute'
        ELSE 'Unknown'
    END                                                     AS payment_method,
    CASE
        WHEN trip_distance < 1   THEN '< 1 mile'
        WHEN trip_distance < 3   THEN '1–3 miles'
        WHEN trip_distance < 10  THEN '3–10 miles'
        ELSE '10+ miles'
    END                                                     AS distance_band,
    COUNT(*)                                                AS total_trips,
    ROUND(AVG(tip_pct), 2)                                  AS avg_tip_pct,
    ROUND(AVG(tip_amount), 2)                               AS avg_tip_amount,
    ROUND(MEDIAN(tip_pct), 2)                               AS median_tip_pct,
    ROUND(STDDEV(tip_pct), 2)                               AS stddev_tip_pct
FROM trips
GROUP BY payment_method, distance_band
ORDER BY payment_method, distance_band;
