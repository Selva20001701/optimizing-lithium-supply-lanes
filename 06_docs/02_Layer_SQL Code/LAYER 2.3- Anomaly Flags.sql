DROP VIEW IF EXISTS public.vw_shipment_anomaly_flags;

CREATE VIEW public.vw_shipment_anomaly_flags AS

WITH lane_baselines AS (
    -- Calculate per-lane baselines for comparison
    SELECT
        lane_id,
        AVG(actual_cost)                        AS lane_avg_cost,
        STDDEV(actual_cost)                     AS lane_stddev_cost,
        AVG(actual_transit_days::NUMERIC)        AS lane_avg_transit,
        STDDEV(actual_transit_days::NUMERIC)     AS lane_stddev_transit,
        AVG(cost_leakage_amount)                AS lane_avg_leakage,
        STDDEV(cost_leakage_amount)             AS lane_stddev_leakage
    FROM public.shipments
    GROUP BY lane_id
)
SELECT
    s.shipment_id,
    s.shipment_date,
    s.year,
    s.month,
    s.month_name,
    s.lane_id,
    s.origin_state,
    s.destination_state,
    s.carrier_id,
    s.carrier_name,
    s.distance_miles,
    s.quoted_cost,
    s.actual_cost,
    s.cost_leakage_amount,
    s.planned_transit_days,
    s.actual_transit_days,
    s.on_time_flag,
    s.invoice_exception_flag,
    s.utilization_pct,
    s.priority_flag,

    -- Cost deviation from lane average (in standard deviations)
    ROUND(
        (s.actual_cost - b.lane_avg_cost) / NULLIF(b.lane_stddev_cost, 0), 2
    )                                           AS cost_z_score,

    -- Transit deviation from lane average (in standard deviations)
    ROUND(
        (s.actual_transit_days - b.lane_avg_transit) / NULLIF(b.lane_stddev_transit, 0), 2
    )                                           AS transit_z_score,

    -- Leakage deviation from lane average
    ROUND(
        (s.cost_leakage_amount - b.lane_avg_leakage) / NULLIF(b.lane_stddev_leakage, 0), 2
    )                                           AS leakage_z_score,

    -- === ANOMALY FLAGS ===

    -- Cost anomaly: actual cost > 2 standard deviations above lane mean
    CASE
        WHEN (s.actual_cost - b.lane_avg_cost) / NULLIF(b.lane_stddev_cost, 0) > 2.0
        THEN 1 ELSE 0
    END                                         AS cost_anomaly_flag,

    -- Transit anomaly: actual transit > 2 standard deviations above lane mean
    CASE
        WHEN (s.actual_transit_days - b.lane_avg_transit) / NULLIF(b.lane_stddev_transit, 0) > 2.0
        THEN 1 ELSE 0
    END                                         AS transit_anomaly_flag,

    -- Leakage anomaly: cost leakage > 2 standard deviations above lane mean
    CASE
        WHEN (s.cost_leakage_amount - b.lane_avg_leakage) / NULLIF(b.lane_stddev_leakage, 0) > 2.0
        THEN 1 ELSE 0
    END                                         AS leakage_anomaly_flag,

    -- Low utilization anomaly: below 50%
    CASE
        WHEN s.utilization_pct < 0.50
        THEN 1 ELSE 0
    END                                         AS low_util_anomaly_flag,

    -- Combined anomaly: any anomaly present
    CASE
        WHEN (s.actual_cost - b.lane_avg_cost) / NULLIF(b.lane_stddev_cost, 0) > 2.0
          OR (s.actual_transit_days - b.lane_avg_transit) / NULLIF(b.lane_stddev_transit, 0) > 2.0
          OR (s.cost_leakage_amount - b.lane_avg_leakage) / NULLIF(b.lane_stddev_leakage, 0) > 2.0
          OR s.utilization_pct < 0.50
        THEN 1 ELSE 0
    END                                         AS any_anomaly_flag,

    -- Anomaly type label
    CONCAT_WS(' | ',
        CASE WHEN (s.actual_cost - b.lane_avg_cost) / NULLIF(b.lane_stddev_cost, 0) > 2.0
             THEN 'Cost Spike' END,
        CASE WHEN (s.actual_transit_days - b.lane_avg_transit) / NULLIF(b.lane_stddev_transit, 0) > 2.0
             THEN 'Transit Delay' END,
        CASE WHEN (s.cost_leakage_amount - b.lane_avg_leakage) / NULLIF(b.lane_stddev_leakage, 0) > 2.0
             THEN 'Leakage Spike' END,
        CASE WHEN s.utilization_pct < 0.50
             THEN 'Low Utilization' END
    )                                           AS anomaly_types

FROM public.shipments s
JOIN lane_baselines b ON s.lane_id = b.lane_id;

-- =============================================================================
-- Verify: all shipments with anomalies
-- =============================================================================
SELECT * FROM public.vw_shipment_anomaly_flags
WHERE any_anomaly_flag = 1
ORDER BY shipment_date DESC;

-- =============================================================================
-- Summary: anomaly counts by type
-- =============================================================================
SELECT
    SUM(cost_anomaly_flag)          AS cost_anomalies,
    SUM(transit_anomaly_flag)       AS transit_anomalies,
    SUM(leakage_anomaly_flag)       AS leakage_anomalies,
    SUM(low_util_anomaly_flag)      AS low_util_anomalies,
    SUM(any_anomaly_flag)           AS total_anomalous_shipments,
    COUNT(*)                        AS total_shipments,
    ROUND(SUM(any_anomaly_flag)::NUMERIC / COUNT(*) * 100, 2) AS anomaly_rate_pct
FROM public.vw_shipment_anomaly_flags;

-- =============================================================================
-- Summary: anomaly counts by lane
-- =============================================================================
SELECT
    lane_id,
    origin_state,
    destination_state,
    COUNT(*)                        AS total_shipments,
    SUM(any_anomaly_flag)           AS anomalous_shipments,
    ROUND(SUM(any_anomaly_flag)::NUMERIC / COUNT(*) * 100, 2) AS anomaly_rate_pct,
    SUM(cost_anomaly_flag)          AS cost_anomalies,
    SUM(transit_anomaly_flag)       AS transit_anomalies,
    SUM(leakage_anomaly_flag)       AS leakage_anomalies,
    SUM(low_util_anomaly_flag)      AS low_util_anomalies
FROM public.vw_shipment_anomaly_flags
GROUP BY lane_id, origin_state, destination_state
ORDER BY anomaly_rate_pct DESC;

-- =============================================================================
-- Summary: anomaly counts by carrier
-- =============================================================================
SELECT
    carrier_id,
    carrier_name,
    COUNT(*)                        AS total_shipments,
    SUM(any_anomaly_flag)           AS anomalous_shipments,
    ROUND(SUM(any_anomaly_flag)::NUMERIC / COUNT(*) * 100, 2) AS anomaly_rate_pct,
    SUM(cost_anomaly_flag)          AS cost_anomalies,
    SUM(transit_anomaly_flag)       AS transit_anomalies
FROM public.vw_shipment_anomaly_flags
GROUP BY carrier_id, carrier_name
ORDER BY anomaly_rate_pct DESC;