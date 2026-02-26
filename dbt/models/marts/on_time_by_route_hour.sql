/*
Mart: On-time performance aggregated by route × hour of day × day of week.

This is the primary table powering the heatmap in the Streamlit dashboard.
"On time" = CTA did not flag the vehicle as delayed.

Since we're polling predictions every minute, a single train trip generates
many rows. We deduplicate by taking one reading per (run_number, predicted_arrival)
pair — the most recent prediction we captured for that specific arrival.
*/

with train_deduped as (
    -- One row per unique (run, arrival). Use the latest prediction we saw.
    select distinct on (run_number, predicted_arrival)
        route,
        stop_id,
        station_name,
        predicted_arrival,
        is_delayed,
        hour_of_day,
        day_of_week,
        day_name,
        'train' as mode
    from {{ ref('stg_train_arrivals') }}
    order by run_number, predicted_arrival, collected_at desc
),

bus_deduped as (
    -- One row per unique (vehicle, stop, arrival).
    select distinct on (vehicle_id, stop_id, predicted_arrival)
        route,
        stop_id,
        stop_name     as station_name,
        predicted_arrival,
        is_delayed,
        hour_of_day,
        day_of_week,
        day_name,
        'bus' as mode
    from {{ ref('stg_bus_predictions') }}
    order by vehicle_id, stop_id, predicted_arrival, collected_at desc
),

combined as (
    select * from train_deduped
    union all
    select * from bus_deduped
),

aggregated as (
    select
        mode,
        route,
        hour_of_day,
        day_of_week,
        day_name,
        count(*)                                            as observation_count,
        sum(case when not is_delayed then 1 else 0 end)    as on_time_count,
        round(
            100.0 * sum(case when not is_delayed then 1 else 0 end) / count(*),
            1
        )                                                   as pct_on_time,
        round(
            100.0 * sum(case when is_delayed then 1 else 0 end) / count(*),
            1
        )                                                   as pct_delayed
    from combined
    group by 1, 2, 3, 4, 5
)

select * from aggregated
order by mode, route, day_of_week, hour_of_day
