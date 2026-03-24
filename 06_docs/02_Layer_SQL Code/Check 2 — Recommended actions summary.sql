SELECT lane_id, primary_action, action_priority, performance_quadrant, estimated_savings
FROM public.vw_lane_recommended_actions
ORDER BY action_priority, lane_id;