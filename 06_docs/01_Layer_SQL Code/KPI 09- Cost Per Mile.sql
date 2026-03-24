-- 9A: Cost per mile by lane
SELECT
    lane_id,
    origin_state,
    destination_state,
    distance_miles,
    COUNT(*)                                            AS total_shipments,
    ROUND(AVG(actual_cost), 2)                          AS avg_actual_cost,
    ROUND(AVG(actual_cost) / NULLIF(distance_miles, 0), 2)  AS avg_cost_per_mile,
    ROUND(AVG(quoted_cost) / NULLIF(distance_miles, 0), 2)  AS avg_quoted_cost_per_mile
FROM public.shipments
GROUP BY lane_id, origin_state, destination_state, distance_miles
ORDER BY avg_cost_per_mile DESC;

-- 9B: Cost per mile by carrier
SELECT
    carrier_id,
    carrier_name,
    COUNT(*)                                            AS total_shipments,
    ROUND(SUM(actual_cost) / NULLIF(SUM(distance_miles), 0), 2) AS avg_cost_per_mile,
    ROUND(SUM(quoted_cost) / NULLIF(SUM(distance_miles), 0), 2) AS avg_quoted_cost_per_mile
FROM public.shipments
GROUP BY carrier_id, carrier_name
ORDER BY avg_cost_per_mile DESC;

-- 9C: Cost per mile by lane and carrier
SELECT
    lane_id,
    carrier_id,
    carrier_name,
    distance_miles,
    COUNT(*)                                            AS total_shipments,
    ROUND(AVG(actual_cost) / NULLIF(distance_miles, 0), 2)  AS avg_cost_per_mile
FROM public.shipments
GROUP BY lane_id, carrier_id, carrier_name, distance_miles
ORDER BY lane_id, avg_cost_per_mile DESC;