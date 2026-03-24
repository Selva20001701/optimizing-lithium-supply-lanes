SELECT
    carrier_id,
    carrier_name,
    carrier_profile,
    COUNT(*)                                        AS total_shipments,
    ROUND(SUM(actual_cost), 2)                      AS total_actual_spend,
    ROUND(AVG(quoted_cost), 2)                      AS avg_quoted_cost,
    ROUND(AVG(actual_cost), 2)                      AS avg_actual_cost,
    ROUND(AVG(cost_leakage_amount), 2)              AS avg_cost_leakage,
    ROUND(SUM(cost_leakage_amount), 2)              AS total_cost_leakage,
    ROUND(AVG(on_time_flag::NUMERIC), 4)            AS otif_rate,
    ROUND(AVG(invoice_exception_flag::NUMERIC), 4)  AS exception_rate,
    ROUND(AVG(utilization_pct), 2)                  AS avg_utilization,
    ROUND(AVG(actual_transit_days::NUMERIC), 2)     AS avg_actual_transit_days,
    COUNT(DISTINCT lane_id)                         AS lanes_served
FROM public.shipments
GROUP BY carrier_id, carrier_name, carrier_profile
ORDER BY total_actual_spend DESC;