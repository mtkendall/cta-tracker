/*
Mart: Per-arrival headway (minutes between consecutive vehicle arrivals at a stop).

Uses LAG(predicted_arrival) partitioned by (mode, route, stop_id) to compute the
gap between consecutive predicted arrivals — which is the headway a rider experiences.
Gaps > 120 minutes are excluded (service gaps / data outages, not real headways).
*/

with ordered as (
    select
        mode,
        route,
        stop_id,
        stop_name,
        predicted_arrival,
        collected_at,
        date_trunc('day', collected_at)::date as collected_date,
        hour_of_day,
        day_of_week,
        day_name,
        lag(predicted_arrival) over (
            partition by mode, route, stop_id
            order by predicted_arrival
        ) as prev_predicted_arrival
    from {{ ref('arrival_history') }}
)

select
    mode,
    route,
    stop_id,
    stop_name,
    predicted_arrival,
    collected_at,
    collected_date,
    hour_of_day,
    day_of_week,
    day_name,
    datediff('minute', prev_predicted_arrival, predicted_arrival) as headway_minutes
from ordered
where prev_predicted_arrival is not null
  and datediff('minute', prev_predicted_arrival, predicted_arrival) between 1 and 120
