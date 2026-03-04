/*
Mart: On-time performance aggregated by route × hour of day × day of week.

This is the primary table powering the heatmap in the Streamlit dashboard.
"On time" = CTA did not flag the vehicle as delayed.

Since we're polling predictions every minute, a single train trip generates
many rows. We deduplicate by taking one reading per (run_number, stop_id)
pair — the most recent prediction we captured for that train's visit to that stop.

Incremental strategy: on each run, new observations are merged into the existing
accumulated counts so that statistics reflect ALL historical data even after raw
tables are trimmed. max_collected_at tracks the watermark for incremental runs.
*/

{{ config(
    materialized='incremental',
    unique_key=['mode', 'route', 'hour_of_day', 'day_of_week'],
    incremental_strategy='delete+insert'
) }}

{% if is_incremental() %}

with watermark as (
    select coalesce(max(max_collected_at), '1970-01-01'::timestamp) as ts
    from {{ this }}
),

new_train as (
    select distinct on (run_number, stop_id, predicted_arrival::date)
        route,
        is_delayed,
        hour_of_day,
        day_of_week,
        day_name,
        'train'     as mode,
        collected_at
    from {{ ref('stg_train_arrivals') }}
    where collected_at > (select ts from watermark)
    order by run_number, stop_id, predicted_arrival::date, collected_at desc
),

new_bus as (
    select distinct on (vehicle_id, stop_id, predicted_arrival)
        route,
        is_delayed,
        hour_of_day,
        day_of_week,
        day_name,
        'bus'       as mode,
        collected_at
    from {{ ref('stg_bus_predictions') }}
    where collected_at > (select ts from watermark)
    order by vehicle_id, stop_id, predicted_arrival, collected_at desc
),

new_combined as (
    select * from new_train
    union all
    select * from new_bus
),

new_aggregated as (
    select
        mode,
        route,
        hour_of_day,
        day_of_week,
        day_name,
        count(*)                                            as new_observation_count,
        sum(case when not is_delayed then 1 else 0 end)    as new_on_time_count,
        max(collected_at)                                   as new_max_collected_at
    from new_combined
    group by mode, route, hour_of_day, day_of_week, day_name
),

-- Full outer join so unchanged rows are preserved and updated rows get merged counts
merged as (
    select
        coalesce(e.mode,        n.mode)        as mode,
        coalesce(e.route,       n.route)       as route,
        coalesce(e.hour_of_day, n.hour_of_day) as hour_of_day,
        coalesce(e.day_of_week, n.day_of_week) as day_of_week,
        coalesce(e.day_name,    n.day_name)    as day_name,
        coalesce(e.observation_count, 0) + coalesce(n.new_observation_count, 0) as observation_count,
        coalesce(e.on_time_count,     0) + coalesce(n.new_on_time_count,     0) as on_time_count,
        case
            when e.max_collected_at    is null then n.new_max_collected_at
            when n.new_max_collected_at is null then e.max_collected_at
            else greatest(e.max_collected_at, n.new_max_collected_at)
        end as max_collected_at
    from {{ this }} e
    full outer join new_aggregated n
        on  e.mode        = n.mode
        and e.route       = n.route
        and e.hour_of_day = n.hour_of_day
        and e.day_of_week = n.day_of_week
)

select
    mode,
    route,
    hour_of_day,
    day_of_week,
    day_name,
    observation_count,
    on_time_count,
    round(100.0 * on_time_count / observation_count, 1)                     as pct_on_time,
    round(100.0 * (observation_count - on_time_count) / observation_count, 1) as pct_delayed,
    max_collected_at
from merged
order by mode, route, day_of_week, hour_of_day

{% else %}

-- Full build: rebuild from all available raw data (first run or --full-refresh)
with train_deduped as (
    select distinct on (run_number, stop_id, predicted_arrival::date)
        route,
        is_delayed,
        hour_of_day,
        day_of_week,
        day_name,
        'train'     as mode,
        collected_at
    from {{ ref('stg_train_arrivals') }}
    order by run_number, stop_id, predicted_arrival::date, collected_at desc
),

bus_deduped as (
    select distinct on (vehicle_id, stop_id, predicted_arrival)
        route,
        is_delayed,
        hour_of_day,
        day_of_week,
        day_name,
        'bus'       as mode,
        collected_at
    from {{ ref('stg_bus_predictions') }}
    order by vehicle_id, stop_id, predicted_arrival, collected_at desc
),

combined as (
    select * from train_deduped
    union all
    select * from bus_deduped
)

select
    mode,
    route,
    hour_of_day,
    day_of_week,
    day_name,
    count(*)                                            as observation_count,
    sum(case when not is_delayed then 1 else 0 end)    as on_time_count,
    round(100.0 * sum(case when not is_delayed then 1 else 0 end) / count(*), 1) as pct_on_time,
    round(100.0 * sum(case when is_delayed     then 1 else 0 end) / count(*), 1) as pct_delayed,
    max(collected_at)                                                             as max_collected_at
from combined
group by mode, route, hour_of_day, day_of_week, day_name
order by mode, route, day_of_week, hour_of_day

{% endif %}
