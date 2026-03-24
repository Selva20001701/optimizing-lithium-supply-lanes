-- 6A: Cost leakage by lane
SELECT
    lane_id,
    origin_state,
    destination_state,
    COUNT(*)                                        AS total_shipments,
    ROUND(SUM(cost_leakage_amount), 2)              AS total_leakage,
    ROUND(AVG(cost_leakage_amount), 2)              AS avg_leakage_per_shipment,
    ROUND(SUM(quoted_cost), 2)                      AS total_quoted,
    ROUND(SUM(actual_cost), 2)                      AS total_actual,
    ROUND(
        (SUM(actual_cost) - SUM(quoted_cost)) 
        / NULLIF(SUM(quoted_cost), 0) * 100, 2
    )                                               AS leakage_pct_of_quoted
FROM public.shipments
GROUP BY lane_id, origin_state, destination_state
ORDER BY total_leakage DESC;

-- 6B: Cost leakage by carrier
SELECT
    carrier_id,
    carrier_name,
    COUNT(*)                                        AS total_shipments,
    ROUND(SUM(cost_leakage_amount), 2)              AS total_leakage,
    ROUND(AVG(cost_leakage_amount), 2)              AS avg_leakage_per_shipment,
    ROUND(
        (SUM(actual_cost) - SUM(quoted_cost)) 
        / NULLIF(SUM(quoted_cost), 0) * 100, 2
    )                                               AS leakage_pct_of_quoted
FROM public.shipments
GROUP BY carrier_id, carrier_name
ORDER BY total_leakage DESC;

-- 6C: Shipments with highest individual cost leakage
SELECT
    shipment_id,
    shipment_date,
    lane_id,
    carrier_name,
    quoted_cost,
    actual_cost,
    cost_leakage_amount,
    ROUND(
        cost_leakage_amount / NULLIF(quoted_cost, 0) * 100, 2
    )                                               AS leakage_pct
FROM public.shipments
WHERE cost_leakage_amount > 0
ORDER BY cost_leakage_amount DESC
LIMIT 20;