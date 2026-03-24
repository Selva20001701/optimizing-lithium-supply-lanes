

-- 7A: Utilization by lane
SELECT
    lane_id,
    origin_state,
    destination_state,
    COUNT(*)                                AS total_shipments,
    ROUND(AVG(utilization_pct), 2)          AS avg_utilization,
    ROUND(MIN(utilization_pct), 2)          AS min_utilization,
    ROUND(MAX(utilization_pct), 2)          AS max_utilization,
    SUM(CASE WHEN utilization_pct < 0.65 THEN 1 ELSE 0 END) AS low_util_shipments,
    ROUND(
        SUM(CASE WHEN utilization_pct < 0.65 THEN 1 ELSE 0 END)::NUMERIC 
        / COUNT(*) * 100, 2
    )                                       AS low_util_rate_pct
FROM public.shipments
GROUP BY lane_id, origin_state, destination_state
ORDER BY avg_utilization ASC;

-- 7B: Utilization by carrier
SELECT
    carrier_id,
    carrier_name,
    COUNT(*)                                AS total_shipments,
    ROUND(AVG(utilization_pct), 2)          AS avg_utilization,
    ROUND(MIN(utilization_pct), 2)          AS min_utilization,
    ROUND(MAX(utilization_pct), 2)          AS max_utilization
FROM public.shipments
GROUP BY carrier_id, carrier_name
ORDER BY avg_utilization ASC;

-- 7C: Utilization distribution buckets
SELECT
    CASE
        WHEN utilization_pct < 0.50 THEN '01: Below 50%'
        WHEN utilization_pct < 0.65 THEN '02: 50-64%'
        WHEN utilization_pct < 0.75 THEN '03: 65-74%'
        WHEN utilization_pct < 0.85 THEN '04: 75-84%'
        WHEN utilization_pct < 0.95 THEN '05: 85-94%'
        ELSE '06: 95-100%'
    END                                     AS utilization_bucket,
    COUNT(*)                                AS shipment_count,
    ROUND(COUNT(*)::NUMERIC / (SELECT COUNT(*) FROM public.shipments) * 100, 2) AS pct_of_total
FROM public.shipments
GROUP BY utilization_bucket
ORDER BY utilization_bucket;