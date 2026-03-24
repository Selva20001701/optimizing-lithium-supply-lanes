SELECT
    lane_id,
    origin_state,
    destination_state,
    COUNT(*)                                            AS total_shipments,
    ROUND(AVG(utilization_pct), 2)                      AS avg_utilization,
    ROUND(AVG(weight_lbs), 0)                           AS avg_weight_lbs,
    SUM(CASE WHEN utilization_pct < 0.65 THEN 1 ELSE 0 END) AS low_util_shipments,
    ROUND(
        SUM(CASE WHEN utilization_pct < 0.65 THEN 1 ELSE 0 END)::NUMERIC
        / COUNT(*), 4
    )                                                   AS low_util_rate,
    ROUND(AVG(actual_cost), 2)                          AS avg_cost,
    -- Estimated savings if low-util shipments were consolidated
    -- Assumption: 30% of low-util shipment costs could be saved
    ROUND(
        SUM(CASE WHEN utilization_pct < 0.65 THEN actual_cost ELSE 0 END) * 0.30, 2
    )                                                   AS estimated_consolidation_savings
FROM public.shipments
GROUP BY lane_id, origin_state, destination_state
ORDER BY estimated_consolidation_savings DESC;

-- 14B: Same-day shipment pairs on same lane (consolidation candidates)
SELECT
    lane_id,
    shipment_date,
    COUNT(*)                                    AS shipments_on_same_day,
    ROUND(AVG(utilization_pct), 2)              AS avg_utilization,
    ROUND(SUM(actual_cost), 2)                  AS total_daily_cost,
    SUM(CASE WHEN utilization_pct < 0.70 THEN 1 ELSE 0 END) AS under_70_util_count
FROM public.shipments
GROUP BY lane_id, shipment_date
HAVING COUNT(*) >= 2
ORDER BY shipments_on_same_day DESC, total_daily_cost DESC
LIMIT 30;

-- 14C: Monthly consolidation opportunity by lane
SELECT
    lane_id,
    year,
    month,
    month_name,
    COUNT(*)                                    AS total_shipments,
    ROUND(AVG(utilization_pct), 2)              AS avg_utilization,
    SUM(CASE WHEN utilization_pct < 0.65 THEN 1 ELSE 0 END) AS low_util_shipments,
    ROUND(
        SUM(CASE WHEN utilization_pct < 0.65 THEN actual_cost ELSE 0 END) * 0.30, 2
    )                                           AS monthly_savings_potential
FROM public.shipments
GROUP BY lane_id, year, month, month_name
ORDER BY lane_id, year, month;