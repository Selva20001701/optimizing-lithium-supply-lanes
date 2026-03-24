-- 8A: Cost per shipment by lane
SELECT
    lane_id,
    origin_state,
    destination_state,
    distance_miles,
    COUNT(*)                                AS total_shipments,
    ROUND(SUM(actual_cost), 2)              AS total_spend,
    ROUND(AVG(actual_cost), 2)              AS avg_cost_per_shipment,
    ROUND(AVG(quoted_cost), 2)              AS avg_quoted_per_shipment,
    ROUND(AVG(actual_cost) - AVG(quoted_cost), 2) AS avg_overrun_per_shipment
FROM public.shipments
GROUP BY lane_id, origin_state, destination_state, distance_miles
ORDER BY avg_cost_per_shipment DESC;

-- 8B: Cost per shipment by carrier
SELECT
    carrier_id,
    carrier_name,
    COUNT(*)                                AS total_shipments,
    ROUND(AVG(actual_cost), 2)              AS avg_cost_per_shipment,
    ROUND(AVG(quoted_cost), 2)              AS avg_quoted_per_shipment,
    ROUND(AVG(actual_cost) - AVG(quoted_cost), 2) AS avg_overrun_per_shipment
FROM public.shipments
GROUP BY carrier_id, carrier_name
ORDER BY avg_cost_per_shipment DESC;

-- 8C: Cost per shipment by lane and carrier
SELECT
    lane_id,
    carrier_id,
    carrier_name,
    COUNT(*)                                AS total_shipments,
    ROUND(AVG(actual_cost), 2)              AS avg_cost_per_shipment,
    ROUND(AVG(quoted_cost), 2)              AS avg_quoted_per_shipment
FROM public.shipments
GROUP BY lane_id, carrier_id, carrier_name
ORDER BY lane_id, avg_cost_per_shipment DESC;