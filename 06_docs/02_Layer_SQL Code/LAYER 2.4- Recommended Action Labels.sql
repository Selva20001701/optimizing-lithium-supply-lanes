DROP VIEW IF EXISTS public.vw_lane_recommended_actions;

CREATE VIEW public.vw_lane_recommended_actions AS

WITH lane_metrics AS (
    SELECT
        lane_id,
        origin_state,
        destination_state,
        distance_miles,
        service_sensitivity,
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
        lane_id, origin_state, destination_state, distance_miles,
        service_sensitivity, strategic_priority
),
network_benchmarks AS (
    -- Overall network averages as comparison thresholds
    SELECT
        AVG(on_time_flag::NUMERIC)              AS network_otif,
        AVG(invoice_exception_flag::NUMERIC)    AS network_exception,
        AVG(cost_leakage_amount)                AS network_leakage,
        AVG(utilization_pct)                    AS network_utilization,
        AVG(actual_cost / NULLIF(distance_miles, 0)) AS network_cpm
    FROM public.shipments
)
SELECT
    lm.lane_id,
    lm.origin_state,
    lm.destination_state,
    lm.distance_miles,
    lm.service_sensitivity,
    lm.strategic_priority,
    lm.total_shipments,
    lm.otif_rate,
    lm.exception_rate,
    lm.avg_leakage,
    lm.total_leakage,
    lm.avg_utilization,
    lm.cost_per_mile,
    lm.avg_transit_variance,

    -- === PRIMARY ACTION ===
    -- Based on the most critical issue for each lane
    CASE
        -- Critical: low OTIF + high exceptions = carrier problem
        WHEN lm.otif_rate < nb.network_otif
         AND lm.exception_rate > nb.network_exception
        THEN 'Shift Carrier Mix'

        -- Low OTIF but exceptions are fine = transit/routing problem
        WHEN lm.otif_rate < nb.network_otif
         AND lm.exception_rate <= nb.network_exception
        THEN 'Improve Transit Planning'

        -- Good OTIF but high cost = cost optimization needed
        WHEN lm.otif_rate >= nb.network_otif
         AND lm.cost_per_mile > nb.network_cpm
         AND lm.avg_utilization < nb.network_utilization
        THEN 'Consolidate Shipments'

        -- Good OTIF, good utilization, but high leakage = invoice issue
        WHEN lm.otif_rate >= nb.network_otif
         AND lm.avg_leakage > nb.network_leakage
        THEN 'Audit Invoice Exceptions'

        -- Low utilization but everything else is acceptable
        WHEN lm.avg_utilization < nb.network_utilization
        THEN 'Improve Load Utilization'

        -- Performing well overall
        ELSE 'Monitor — No Immediate Action'
    END                                         AS primary_action,

    -- === PRIORITY LEVEL ===
    CASE
        WHEN lm.otif_rate < nb.network_otif
         AND lm.exception_rate > nb.network_exception
        THEN 'P1 — Urgent'

        WHEN lm.otif_rate < nb.network_otif
        THEN 'P2 — High'

        WHEN lm.cost_per_mile > nb.network_cpm
          OR lm.avg_leakage > nb.network_leakage
        THEN 'P3 — Medium'

        ELSE 'P4 — Low'
    END                                         AS action_priority,

    -- === PERFORMANCE QUADRANT ===
    CASE
        WHEN lm.cost_per_mile > nb.network_cpm
         AND lm.otif_rate < nb.network_otif
        THEN 'High Cost / Low Service'

        WHEN lm.cost_per_mile > nb.network_cpm
         AND lm.otif_rate >= nb.network_otif
        THEN 'High Cost / Good Service'

        WHEN lm.cost_per_mile <= nb.network_cpm
         AND lm.otif_rate < nb.network_otif
        THEN 'Low Cost / Low Service'

        ELSE 'Low Cost / Good Service'
    END                                         AS performance_quadrant,

    -- === ESTIMATED IMPACT ===
    -- Rough savings estimate based on action type
    GREATEST(0,
        CASE
            WHEN lm.otif_rate < nb.network_otif
             AND lm.exception_rate > nb.network_exception
            THEN ROUND(lm.total_leakage * 0.50, 2)  -- 50% leakage recovery from carrier shift

            WHEN lm.avg_utilization < nb.network_utilization
            THEN ROUND(lm.avg_actual_cost * lm.total_shipments * 0.10, 2)  -- 10% cost reduction from consolidation

            WHEN lm.avg_leakage > nb.network_leakage
            THEN ROUND(lm.total_leakage * 0.40, 2)  -- 40% leakage recovery from invoice audit

            ELSE 0
        END
    )                                           AS estimated_savings

FROM lane_metrics lm
CROSS JOIN network_benchmarks nb
ORDER BY
    CASE
        WHEN lm.otif_rate < nb.network_otif AND lm.exception_rate > nb.network_exception THEN 1
        WHEN lm.otif_rate < nb.network_otif THEN 2
        WHEN lm.cost_per_mile > nb.network_cpm OR lm.avg_leakage > nb.network_leakage THEN 3
        ELSE 4
    END,
    lm.lane_id;

-- =============================================================================
-- Verify the view
-- =============================================================================
SELECT * FROM public.vw_lane_recommended_actions;

-- =============================================================================
-- Summary: actions distribution
-- =============================================================================
SELECT
    primary_action,
    action_priority,
    COUNT(*)                                AS lane_count,
    ROUND(SUM(estimated_savings), 2)        AS total_estimated_savings
FROM public.vw_lane_recommended_actions
GROUP BY primary_action, action_priority
ORDER BY action_priority, primary_action;

-- =============================================================================
-- Summary: performance quadrant distribution
-- =============================================================================
SELECT
    performance_quadrant,
    COUNT(*)                                AS lane_count,
    ROUND(AVG(otif_rate), 4)                AS avg_otif,
    ROUND(AVG(cost_per_mile), 2)            AS avg_cpm,
    ROUND(AVG(exception_rate), 4)           AS avg_exception_rate
FROM public.vw_lane_recommended_actions
GROUP BY performance_quadrant
ORDER BY performance_quadrant;