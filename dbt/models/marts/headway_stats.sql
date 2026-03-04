/*
Mart: Aggregated headway statistics by stop, hour, day, and collection date.

Powers the headway heatmap and trend chart in the dashboard.
Includes collected_date so the app can filter to rolling time windows.

Incremental strategy: append new collected_dates. We always reprocess the last
2 days to ensure the current (partial) day's stats are updated each run.
Historical date rows are preserved even after arrival_history is trimmed.
*/

{{ config(
    materialized='incremental',
    unique_key=['mode', 'route', 'stop_id', 'destination', 'hour_of_day', 'day_of_week', 'collected_date'],
    incremental_strategy='delete+insert'
) }}

{% if is_incremental() %}

select
    mode,
    route,
    stop_id,
    stop_name,
    destination,
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
-- Always reprocess the last 2 days so today's partial stats are updated each run
where collected_date >= (
    select coalesce(max(collected_date), '1970-01-01'::date) - 1 from {{ this }}
)
group by mode, route, stop_id, stop_name, destination, hour_of_day, day_of_week, day_name, collected_date

{% else %}

-- Full build: rebuild from all available data (first run or --full-refresh)
select
    mode,
    route,
    stop_id,
    stop_name,
    destination,
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
group by mode, route, stop_id, stop_name, destination, hour_of_day, day_of_week, day_name, collected_date

{% endif %}
