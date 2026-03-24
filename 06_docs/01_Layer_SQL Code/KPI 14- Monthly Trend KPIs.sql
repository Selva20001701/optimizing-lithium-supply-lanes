-- 14A: Overall monthly trends
SELECT
    year,
    month,
    month_name,
    COUNT(*)                                            AS total_shipments,
    ROUND(SUM(actual_cost), 2)                          AS total_spend,
    ROUND(AVG(actual_cost), 2)                          AS avg_cost_per_shipment,
    ROUND(AVG(on_time_flag::NUMERIC), 4)                AS otif_rate,
    ROUND(AVG(invoice_exception_flag::NUMERIC), 4)      AS exception_rate,
    ROUND(AVG(utilization_pct), 2)                      AS avg_utilization,
    ROUND(SUM(cost_leakage_amount), 2)                  AS total_leakage,
    ROUND(AVG(cost_leakage_amount), 2)                  AS avg_leakage
FROM public.shipments
GROUP BY year, month, month_name
ORDER BY year, month;

-- 14B: Monthly trends by lane
SELECT
    lane_id,
    year,
    month,
    month_name,
    COUNT(*)                                            AS total_shipments,
    ROUND(SUM(actual_cost), 2)                          AS total_spend,
    ROUND(AVG(on_time_flag::NUMERIC), 4)                AS otif_rate,
    ROUND(AVG(invoice_exception_flag::NUMERIC), 4)      AS exception_rate,
    ROUND(AVG(utilization_pct), 2)                      AS avg_utilization
FROM public.shipments
GROUP BY lane_id, year, month, month_name
ORDER BY lane_id, year, month;

-- 14C: Monthly trends by carrier
SELECT
    carrier_id,
    carrier_name,
    year,
    month,
    month_name,
    COUNT(*)                                            AS total_shipments,
    ROUND(SUM(actual_cost), 2)                          AS total_spend,
    ROUND(AVG(on_time_flag::NUMERIC), 4)                AS otif_rate,
    ROUND(AVG(invoice_exception_flag::NUMERIC), 4)      AS exception_rate
FROM public.shipments
GROUP BY carrier_id, carrier_name, year, month, month_name
ORDER BY carrier_id, year, month;

-- 14D: Quarterly summary
SELECT
    year,
    quarter,
    COUNT(*)                                            AS total_shipments,
    ROUND(SUM(actual_cost), 2)                          AS total_spend,
    ROUND(AVG(actual_cost), 2)                          AS avg_cost_per_shipment,
    ROUND(AVG(on_time_flag::NUMERIC), 4)                AS otif_rate,
    ROUND(AVG(invoice_exception_flag::NUMERIC), 4)      AS exception_rate,
    ROUND(AVG(utilization_pct), 2)                      AS avg_utilization,
    ROUND(SUM(cost_leakage_amount), 2)                  AS total_leakage
FROM public.shipments
GROUP BY year, quarter
ORDER BY year, quarter;