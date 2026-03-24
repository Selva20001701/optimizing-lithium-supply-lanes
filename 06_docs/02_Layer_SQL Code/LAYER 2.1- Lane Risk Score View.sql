DROP VIEW IF EXISTS public.vw_lane_risk_score;

CREATE VIEW public.vw_lane_risk_score AS

WITH lane_metrics AS (
    SELECT
        lane_id,
        origin_state,
        destination_state,
        origin_type,
        destination_type,
        distance_miles,
        volume_band,
        service_sensitivity,
        base_risk_band,
        strategic_priority,
        COUNT(*)                                            AS total_shipments,
        ROUND(AVG(on_time_flag::NUMERIC), 4)                AS otif_rate,
        ROUND(AVG(invoice_exception_flag::NUMERIC), 4)      AS exception_rate,
        ROUND(AVG(cost_leakage_amount), 2)                  AS avg_leakage,
        ROUND(SUM(cost_leakage_amount), 2)                  AS total_leakage,
        ROUND(AVG(utilization_pct), 2)                      AS avg_utilization,
        ROUND(AVG(actual_cost), 2)                          AS avg_actual_cost,
        ROUND(AVG(actual_cost) / NULLIF(distance_miles, 0), 2) AS cost_per_mile,
        ROUND(AVG(actual_transit_days - planned_transit_days::NUMERIC), 2) AS avg_transit_variance
    FROM public.shipments
    GROUP BY
        lane_id, origin_state, destination_state,
        origin_type, destination_type, distance_miles,
        volume_band, service_sensitivity, base_risk_band, strategic_priority
),
normalized_scores AS (
    SELECT
        *,
        -- Service risk: lower OTIF = higher risk (invert)
        ROUND(1 - otif_rate, 4)                             AS service_risk,
        -- Control risk: higher exceptions = higher risk
        ROUND(exception_rate, 4)                            AS control_risk,
        -- Cost risk: normalize leakage relative to max across lanes
        ROUND(
            avg_leakage / NULLIF(MAX(avg_leakage) OVER (), 0), 4
        )                                                   AS cost_risk,
        -- Efficiency risk: lower utilization = higher risk (invert)
        ROUND(1 - avg_utilization, 4)                       AS efficiency_risk
    FROM lane_metrics
)
SELECT
    lane_id,
    origin_state,
    destination_state,
    origin_type,
    destination_type,
    distance_miles,
    volume_band,
    service_sensitivity,
    base_risk_band,
    strategic_priority,
    total_shipments,
    otif_rate,
    exception_rate,
    avg_leakage,
    total_leakage,
    avg_utilization,
    avg_actual_cost,
    cost_per_mile,
    avg_transit_variance,
    service_risk,
    control_risk,
    cost_risk,
    efficiency_risk,

    -- Composite lane risk score (weighted)
    -- Weights: service 30%, control 25%, cost 25%, efficiency 20%
    ROUND(
        (service_risk * 0.30)
        + (control_risk * 0.25)
        + (cost_risk * 0.25)
        + (efficiency_risk * 0.20),
    4)                                                      AS lane_risk_score,

    -- Risk rank
    RANK() OVER (
        ORDER BY
            (service_risk * 0.30) + (control_risk * 0.25)
            + (cost_risk * 0.25) + (efficiency_risk * 0.20) DESC
    )                                                       AS lane_risk_rank,

    -- Risk band classification
    CASE
        WHEN (service_risk * 0.30) + (control_risk * 0.25)
             + (cost_risk * 0.25) + (efficiency_risk * 0.20) >= 0.25
        THEN 'Critical'
        WHEN (service_risk * 0.30) + (control_risk * 0.25)
             + (cost_risk * 0.25) + (efficiency_risk * 0.20) >= 0.18
        THEN 'High'
        WHEN (service_risk * 0.30) + (control_risk * 0.25)
             + (cost_risk * 0.25) + (efficiency_risk * 0.20) >= 0.12
        THEN 'Medium'
        ELSE 'Low'
    END                                                     AS lane_risk_band

FROM normalized_scores
ORDER BY lane_risk_score DESC;

-- =============================================================================
-- Verify the view
-- =============================================================================
SELECT * FROM public.vw_lane_risk_score;