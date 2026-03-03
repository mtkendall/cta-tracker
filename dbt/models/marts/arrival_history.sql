/*
Mart: Flat fact table of all collected arrival predictions.

One row per unique (vehicle/run + stop + predicted_arrival), keeping the
most recent prediction snapshot. Used for the time-series "rolling on-time %"
chart and the raw history table in the dashboard.

Deduplication uses an "islands and gaps" approach: predictions for the same
vehicle/run at the same stop are grouped together if their predicted arrival
times are within 1 hour of each other. The most recent snapshot (by
collected_at) is kept for each group.
*/

with train_gaps as (
    select *,
        case
            when predicted_arrival - lag(predicted_arrival) over (
                partition by run_number, stop_id
                order by predicted_arrival
            ) <= interval '1 hour'
            then 0
            else 1  -- first row (NULL lag) or gap > 1 hour starts a new group
        end as is_new_group
    from {{ ref('stg_train_arrivals') }}
),

train_groups as (
    select *,
        sum(is_new_group) over (
            partition by run_number, stop_id
            order by predicted_arrival
            rows between unbounded preceding and current row
        ) as group_id
    from train_gaps
),

train as (
    select distinct on (run_number, stop_id, group_id)
        'train'         as mode,
        run_number,
        null            as vehicle_id,
        route,
        station_name    as stop_name,
        stop_id,
        dest_name       as destination,
        predicted_arrival,
        prediction_made_at,
        collected_at,
        is_delayed,
        hour_of_day,
        day_of_week,
        day_name,
        minutes_away
    from train_groups
    order by run_number, stop_id, group_id, collected_at desc
),

bus_gaps as (
    select *,
        case
            when predicted_arrival - lag(predicted_arrival) over (
                partition by vehicle_id, stop_id
                order by predicted_arrival
            ) <= interval '1 hour'
            then 0
            else 1
        end as is_new_group
    from {{ ref('stg_bus_predictions') }}
),

bus_groups as (
    select *,
        sum(is_new_group) over (
            partition by vehicle_id, stop_id
            order by predicted_arrival
            rows between unbounded preceding and current row
        ) as group_id
    from bus_gaps
),

bus as (
    select distinct on (vehicle_id, stop_id, group_id)
        'bus'           as mode,
        null            as run_number,
        vehicle_id,
        route,
        stop_name,
        stop_id,
        route_direction as destination,
        predicted_arrival,
        prediction_made_at,
        collected_at,
        is_delayed,
        hour_of_day,
        day_of_week,
        day_name,
        minutes_away
    from bus_groups
    order by vehicle_id, stop_id, group_id, collected_at desc
),

combined as (
    select * from train
    union all
    select * from bus
)

select * from combined
order by collected_at desc
