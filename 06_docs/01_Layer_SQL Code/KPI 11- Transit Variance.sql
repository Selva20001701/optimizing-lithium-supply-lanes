SELECT
    lane_id,
    origin_state,
    destination_state,
    planned_transit_days,
    COUNT(*)                                                    AS total_shipments,
    ROUND(AVG(actual_transit_days::NUMERIC), 2)                 AS avg_actual_transit,
    ROUND(AVG(actual_transit_days - planned_transit_days::NUMERIC), 2) AS avg_transit_variance,
    MAX(actual_transit_days - planned_transit_days)              AS max_delay_days,
    SUM(CASE WHEN actual_transit_days > planned_transit_days THEN 1 ELSE 0 END) AS delayed_shipments,
    ROUND(
        SUM(CASE WHEN actual_transit_days > planned_transit_days THEN 1 ELSE 0 END)::NUMERIC
        / COUNT(*), 4
    )                                                           AS delay_rate
FROM public.shipments
GROUP BY lane_id, origin_state, destination_state, planned_transit_days
ORDER BY avg_transit_variance DESC;

-- 11B: Transit variance by carrier
SELECT
    carrier_id,
    carrier_name,
    COUNT(*)                                                    AS total_shipments,
    ROUND(AVG(actual_transit_days - planned_transit_days::NUMERIC), 2) AS avg_transit_variance,
    MAX(actual_transit_days - planned_transit_days)              AS max_delay_days,
    SUM(CASE WHEN actual_transit_days > planned_transit_days THEN 1 ELSE 0 END) AS delayed_shipments,
    ROUND(
        SUM(CASE WHEN actual_transit_days > planned_transit_days THEN 1 ELSE 0 END)::NUMERIC
        / COUNT(*), 4
    )                                                           AS delay_rate
FROM public.shipments
GROUP BY carrier_id, carrier_name
ORDER BY avg_transit_variance DESC;

-- 11C: Transit variance distribution
SELECT
    actual_transit_days - planned_transit_days       AS variance_days,
    COUNT(*)                                        AS shipment_count,
    CASE
        WHEN actual_transit_days - planned_transit_days < 0 THEN 'Early'
        WHEN actual_transit_days - planned_transit_days = 0 THEN 'On Time'
        ELSE 'Late'
    END                                             AS status
FROM public.shipments
GROUP BY variance_days
ORDER BY variance_days;