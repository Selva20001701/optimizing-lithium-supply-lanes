-- 4A: OTIF by lane
SELECT
    lane_id,
    origin_state,
    destination_state,
    COUNT(*)                                    AS total_shipments,
    SUM(on_time_flag)                           AS on_time_shipments,
    COUNT(*) - SUM(on_time_flag)                AS late_shipments,
    ROUND(AVG(on_time_flag::NUMERIC) * 100, 2)  AS otif_rate_pct
FROM public.shipments
GROUP BY lane_id, origin_state, destination_state
ORDER BY otif_rate_pct ASC;

-- 4B: OTIF by carrier
SELECT
    carrier_id,
    carrier_name,
    COUNT(*)                                    AS total_shipments,
    SUM(on_time_flag)                           AS on_time_shipments,
    COUNT(*) - SUM(on_time_flag)                AS late_shipments,
    ROUND(AVG(on_time_flag::NUMERIC) * 100, 2)  AS otif_rate_pct
FROM public.shipments
GROUP BY carrier_id, carrier_name
ORDER BY otif_rate_pct ASC;

-- 4C: OTIF by lane and carrier (cross-view)
SELECT
    lane_id,
    carrier_id,
    carrier_name,
    COUNT(*)                                    AS total_shipments,
    ROUND(AVG(on_time_flag::NUMERIC) * 100, 2)  AS otif_rate_pct
FROM public.shipments
GROUP BY lane_id, carrier_id, carrier_name
ORDER BY lane_id, otif_rate_pct ASC;