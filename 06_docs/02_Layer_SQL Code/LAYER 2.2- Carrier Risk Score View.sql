DROP VIEW IF EXISTS public.vw_carrier_risk_score;

CREATE VIEW public.vw_carrier_risk_score AS

WITH carrier_metrics AS (
    SELECT
        carrier_id,
        carrier_name,
        carrier_profile,
        COUNT(*)                                            AS total_shipments,
        COUNT(DISTINCT lane_id)                             AS lanes_served,
        ROUND(SUM(actual_cost), 2)                          AS total_spend,
        ROUND(AVG(actual_cost), 2)                          AS avg_actual_cost,
        ROUND(AVG(on_time_flag::NUMERIC), 4)                AS otif_rate,
        ROUND(AVG(invoice_exception_flag::NUMERIC), 4)      AS exception_rate,
        ROUND(AVG(cost_leakage_amount), 2)                  AS avg_leakage,
        ROUND(SUM(cost_leakage_amount), 2)                  AS total_leakage,
        ROUND(AVG(utilization_pct), 2)                      AS avg_utilization,
        ROUND(AVG(actual_transit_days - planned_transit_days::NUMERIC), 2) AS avg_transit_variance,
        ROUND(
            SUM(CASE WHEN actual_transit_days > planned_transit_days THEN 1 ELSE 0 END)::NUMERIC
            / COUNT(*), 4
        )                                                   AS delay_rate
    FROM public.shipments
    GROUP BY carrier_id, carrier_name, carrier_profile
),
normalized_scores AS (
    SELECT
        *,
        -- Service risk: lower OTIF = higher risk
        ROUND(1 - otif_rate, 4)                             AS service_risk,
        -- Control risk: higher exceptions = higher risk
        ROUND(exception_rate, 4)                            AS control_risk,
        -- Cost risk: normalize leakage relative to max
        ROUND(
            avg_leakage / NULLIF(MAX(avg_leakage) OVER (), 0), 4
        )                                                   AS cost_risk,
        -- Reliability risk: higher delay rate = higher risk
        ROUND(delay_rate, 4)                                AS reliability_risk
    FROM carrier_metrics
)
SELECT
    carrier_id,
    carrier_name,
    carrier_profile,
    total_shipments,
    lanes_served,
    total_spend,
    avg_actual_cost,
    otif_rate,
    exception_rate,
    avg_leakage,
    total_leakage,
    avg_utilization,
    avg_transit_variance,
    delay_rate,
    service_risk,
    control_risk,
    cost_risk,
    reliability_risk,

    -- Composite carrier risk score (weighted)
    -- Weights: service 30%, control 25%, cost 25%, reliability 20%
    ROUND(
        (service_risk * 0.30)
        + (control_risk * 0.25)
        + (cost_risk * 0.25)
        + (reliability_risk * 0.20),
    4)                                                      AS carrier_risk_score,

    -- Risk rank
    RANK() OVER (
        ORDER BY
            (service_risk * 0.30) + (control_risk * 0.25)
            + (cost_risk * 0.25) + (reliability_risk * 0.20) DESC
    )                                                       AS carrier_risk_rank,

    -- Risk band classification
    CASE
        WHEN (service_risk * 0.30) + (control_risk * 0.25)
             + (cost_risk * 0.25) + (reliability_risk * 0.20) >= 0.20
        THEN 'High Risk'
        WHEN (service_risk * 0.30) + (control_risk * 0.25)
             + (cost_risk * 0.25) + (reliability_risk * 0.20) >= 0.12
        THEN 'Medium Risk'
        ELSE 'Low Risk'
    END                                                     AS carrier_risk_band

FROM normalized_scores
ORDER BY carrier_risk_score DESC;

-- =============================================================================
-- Verify the view
-- =============================================================================
SELECT * FROM public.vw_carrier_risk_score;