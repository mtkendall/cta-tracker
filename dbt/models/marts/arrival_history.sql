/*
Mart: Flat fact table of all collected arrival predictions.

One row per unique (vehicle/run + stop + predicted_arrival), keeping the
most recent prediction snapshot. Used for the time-series "rolling on-time %"
chart and the raw history table in the dashboard.
*/

with train as (
    select distinct on (run_number, predicted_arrival)
        'train'         as mode,
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
    from {{ ref('stg_train_arrivals') }}
    order by run_number, predicted_arrival, collected_at desc
),

bus as (
    select distinct on (vehicle_id, stop_id, predicted_arrival)
        'bus'           as mode,
        route,
        stop_name,
        stop_id,
        destination,
        predicted_arrival,
        prediction_made_at,
        collected_at,
        is_delayed,
        hour_of_day,
        day_of_week,
        day_name,
        minutes_away
    from {{ ref('stg_bus_predictions') }}
    order by vehicle_id, stop_id, predicted_arrival, collected_at desc
),

combined as (
    select * from train
    union all
    select * from bus
)

select * from combined
order by collected_at desc
