/*
Mart: Aggregated headway statistics by stop, hour, day, and collection date.

Powers the headway heatmap and trend chart in the dashboard.
Includes collected_date so the app can filter to rolling time windows.
*/

select
    mode,
    route,
    stop_id,
    stop_name,
    hour_of_day,
    day_of_week,
    day_name,
    collected_date,
    count(*)                                                               as observation_count,
    round(avg(headway_minutes), 1)                                         as avg_headway_minutes,
    round(
        percentile_cont(0.9) within group (order by headway_minutes), 1
    )                                                                      as p90_headway_minutes,
    max(headway_minutes)                                                   as max_headway_minutes
from {{ ref('headways') }}
group by mode, route, stop_id, stop_name, hour_of_day, day_of_week, day_name, collected_date
