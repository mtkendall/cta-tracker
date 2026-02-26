/*
Staging model for raw bus arrival predictions.
Cleans and enriches bus prediction data.
*/

with source as (
    select * from {{ source('raw', 'raw_bus_predictions') }}
),

cleaned as (
    select
        vehicle_id,
        route,
        route_direction,
        stop_id,
        stop_name,
        destination,
        predicted_arrival,
        prediction_made_at,
        collected_at,
        is_delayed,
        prediction_type,  -- 'A' = arrival, 'D' = departure

        -- Minutes from when prediction was made until predicted arrival
        datediff('minute', prediction_made_at, predicted_arrival) as minutes_away,

        -- Time dimensions for aggregation
        date_trunc('hour', prediction_made_at)  as prediction_hour,
        dayofweek(prediction_made_at)           as day_of_week,
        hour(prediction_made_at)                as hour_of_day,

        case dayofweek(prediction_made_at)
            when 0 then 'Sunday'
            when 1 then 'Monday'
            when 2 then 'Tuesday'
            when 3 then 'Wednesday'
            when 4 then 'Thursday'
            when 5 then 'Friday'
            when 6 then 'Saturday'
        end as day_name

    from source
    -- Only keep arrival predictions (not departure)
    where prediction_type = 'A'
      and minutes_away between 0 and 90
)

select * from cleaned
