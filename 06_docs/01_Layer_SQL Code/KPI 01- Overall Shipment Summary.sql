SELECT
    COUNT(*)                                    AS total_shipments,
    COUNT(DISTINCT lane_id)                     AS total_lanes,
    COUNT(DISTINCT carrier_id)                  AS total_carriers,
    MIN(shipment_date)                          AS first_shipment_date,
    MAX(shipment_date)                          AS last_shipment_date,
    ROUND(SUM(quoted_cost), 2)                  AS total_quoted_spend,
    ROUND(SUM(actual_cost), 2)                  AS total_actual_spend,
    ROUND(SUM(cost_leakage_amount), 2)          AS total_cost_leakage,
    ROUND(AVG(quoted_cost), 2)                  AS avg_quoted_cost,
    ROUND(AVG(actual_cost), 2)                  AS avg_actual_cost,
    ROUND(AVG(weight_lbs), 0)                   AS avg_weight_lbs,
    ROUND(AVG(utilization_pct), 2)              AS avg_utilization,
    ROUND(AVG(on_time_flag::NUMERIC), 4)        AS overall_otif_rate,
    ROUND(AVG(invoice_exception_flag::NUMERIC), 4) AS overall_exception_rate
FROM public.shipments;