SELECT
    lane_id,
    origin_state,
    destination_state,
    origin_type,
    destination_type,
    distance_miles,
    volume_band,
    service_sensitivity,
    strategic_priority,
    COUNT(*)                                        AS total_shipments,
    ROUND(SUM(actual_cost), 2)                      AS total_actual_spend,
    ROUND(AVG(quoted_cost), 2)                      AS avg_quoted_cost,
    ROUND(AVG(actual_cost), 2)                      AS avg_actual_cost,
    ROUND(AVG(cost_leakage_amount), 2)              AS avg_cost_leakage,
    ROUND(AVG(on_time_flag::NUMERIC), 4)            AS otif_rate,
    ROUND(AVG(invoice_exception_flag::NUMERIC), 4)  AS exception_rate,
    ROUND(AVG(utilization_pct), 2)                  AS avg_utilization,
    ROUND(AVG(actual_transit_days::NUMERIC), 2)     AS avg_actual_transit_days
FROM public.shipments
GROUP BY
    lane_id, origin_state, destination_state,
    origin_type, destination_type, distance_miles,
    volume_band, service_sensitivity, strategic_priority
ORDER BY total_actual_spend DESC;