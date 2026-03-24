SELECT
    lane_id,
    origin_state,
    destination_state,
    distance_miles,
    service_sensitivity,
    strategic_priority,
    COUNT(*)                                            AS total_shipments,
    ROUND(AVG(actual_cost), 2)                          AS avg_actual_cost,
    ROUND(AVG(actual_cost) / NULLIF(distance_miles, 0), 2) AS cost_per_mile,
    ROUND(AVG(on_time_flag::NUMERIC), 4)                AS otif_rate,
    ROUND(AVG(invoice_exception_flag::NUMERIC), 4)      AS exception_rate,
    ROUND(AVG(utilization_pct), 2)                      AS avg_utilization,
    ROUND(AVG(cost_leakage_amount), 2)                  AS avg_leakage,
    -- Cost-to-service score: higher = worse
    -- Formula: (cost_per_mile * exception_rate) / OTIF_rate
    -- Lanes with high cost, high exceptions, and low OTIF score worst
    ROUND(
        (AVG(actual_cost) / NULLIF(distance_miles, 0))
        * (1 + AVG(invoice_exception_flag::NUMERIC))
        / NULLIF(AVG(on_time_flag::NUMERIC), 0),
    2)                                                  AS cost_to_service_score
FROM public.shipments
GROUP BY lane_id, origin_state, destination_state, distance_miles,
         service_sensitivity, strategic_priority
ORDER BY cost_to_service_score DESC;

-- 10B: Cost-to-service by carrier
SELECT
    carrier_id,
    carrier_name,
    carrier_profile,
    COUNT(*)                                            AS total_shipments,
    ROUND(AVG(actual_cost), 2)                          AS avg_actual_cost,
    ROUND(AVG(on_time_flag::NUMERIC), 4)                AS otif_rate,
    ROUND(AVG(invoice_exception_flag::NUMERIC), 4)      AS exception_rate,
    ROUND(AVG(cost_leakage_amount), 2)                  AS avg_leakage,
    ROUND(
        AVG(actual_cost)
        * (1 + AVG(invoice_exception_flag::NUMERIC))
        / NULLIF(AVG(on_time_flag::NUMERIC), 0),
    2)                                                  AS cost_to_service_score
FROM public.shipments
GROUP BY carrier_id, carrier_name, carrier_profile
ORDER BY cost_to_service_score DESC;

-- 10C: Quadrant classification by lane
-- Classifies lanes into 4 quadrants based on cost and service
SELECT
    lane_id,
    origin_state,
    destination_state,
    ROUND(AVG(actual_cost) / NULLIF(distance_miles, 0), 2) AS cost_per_mile,
    ROUND(AVG(on_time_flag::NUMERIC), 4)                   AS otif_rate,
    CASE
        WHEN AVG(actual_cost) / NULLIF(distance_miles, 0) > (SELECT AVG(actual_cost / NULLIF(distance_miles, 0)) FROM public.shipments)
         AND AVG(on_time_flag::NUMERIC) < (SELECT AVG(on_time_flag::NUMERIC) FROM public.shipments)
        THEN 'High Cost / Low Service — ACTION NEEDED'
        
        WHEN AVG(actual_cost) / NULLIF(distance_miles, 0) > (SELECT AVG(actual_cost / NULLIF(distance_miles, 0)) FROM public.shipments)
         AND AVG(on_time_flag::NUMERIC) >= (SELECT AVG(on_time_flag::NUMERIC) FROM public.shipments)
        THEN 'High Cost / Good Service — Monitor Cost'
        
        WHEN AVG(actual_cost) / NULLIF(distance_miles, 0) <= (SELECT AVG(actual_cost / NULLIF(distance_miles, 0)) FROM public.shipments)
         AND AVG(on_time_flag::NUMERIC) < (SELECT AVG(on_time_flag::NUMERIC) FROM public.shipments)
        THEN 'Low Cost / Low Service — Improve Service'
        
        ELSE 'Low Cost / Good Service — Best Performers'
    END                                                    AS quadrant
FROM public.shipments
GROUP BY lane_id, origin_state, destination_state, distance_miles
ORDER BY quadrant, lane_id;