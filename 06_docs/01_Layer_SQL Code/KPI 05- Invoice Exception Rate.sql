-- 5A: Exception rate by lane
SELECT
    lane_id,
    origin_state,
    destination_state,
    COUNT(*)                                            AS total_shipments,
    SUM(invoice_exception_flag)                         AS exception_shipments,
    ROUND(AVG(invoice_exception_flag::NUMERIC) * 100, 2) AS exception_rate_pct
FROM public.shipments
GROUP BY lane_id, origin_state, destination_state
ORDER BY exception_rate_pct DESC;

-- 5B: Exception rate by carrier
SELECT
    carrier_id,
    carrier_name,
    COUNT(*)                                            AS total_shipments,
    SUM(invoice_exception_flag)                         AS exception_shipments,
    ROUND(AVG(invoice_exception_flag::NUMERIC) * 100, 2) AS exception_rate_pct
FROM public.shipments
GROUP BY carrier_id, carrier_name
ORDER BY exception_rate_pct DESC;

-- 5C: Exception rate by lane and carrier (cross-view)
SELECT
    lane_id,
    carrier_id,
    carrier_name,
    COUNT(*)                                            AS total_shipments,
    SUM(invoice_exception_flag)                         AS exception_shipments,
    ROUND(AVG(invoice_exception_flag::NUMERIC) * 100, 2) AS exception_rate_pct
FROM public.shipments
GROUP BY lane_id, carrier_id, carrier_name
ORDER BY exception_rate_pct DESC;