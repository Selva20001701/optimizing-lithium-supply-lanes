SELECT lane_id, origin_state, destination_state, lane_risk_score, lane_risk_band
FROM public.vw_lane_risk_score
ORDER BY lane_risk_score DESC;