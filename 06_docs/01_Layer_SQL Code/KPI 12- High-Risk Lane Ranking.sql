WITH lane_metrics AS (
    SELECT
        lane_id,
        origin_state,
        destination_state,
        service_sensitivity,
        strategic_priority,
        COUNT(*)                                            AS total_shipments,
        ROUND(AVG(on_time_flag::NUMERIC), 4)                AS otif_rate,
        ROUND(AVG(invoice_exception_flag::NUMERIC), 4)      AS exception_rate,
        ROUND(AVG(cost_leakage_amount), 2)                  AS avg_leakage,
        ROUND(AVG(utilization_pct), 2)                      AS avg_utilization
    FROM public.shipments
    GROUP BY lane_id, origin_state, destination_state,
             service_sensitivity, strategic_priority
),
risk_scores AS (
    SELECT
        *,
        -- Service risk: lower OTIF = higher risk (invert)
        ROUND(1 - otif_rate, 4)                             AS service_risk,
        -- Control risk: higher exceptions = higher risk
        ROUND(exception_rate, 4)                            AS control_risk,
        -- Cost risk: normalize leakage relative to max
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
    service_sensitivity,
    strategic_priority,
    total_shipments,
    otif_rate,
    exception_rate,
    avg_leakage,
    avg_utilization,
    service_risk,
    control_risk,
    cost_risk,
    efficiency_risk,
    -- Composite risk score (weighted average)
    ROUND(
        (service_risk * 0.30)
        + (control_risk * 0.25)
        + (cost_risk * 0.25)
        + (efficiency_risk * 0.20),
    4)                                                      AS composite_risk_score,
    RANK() OVER (
        ORDER BY (service_risk * 0.30) + (control_risk * 0.25)
                 + (cost_risk * 0.25) + (efficiency_risk * 0.20) DESC
    )                                                       AS risk_rank
FROM risk_scores
ORDER BY composite_risk_score DESC;