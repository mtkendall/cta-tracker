/*
Staging model for raw train arrival predictions.

Cleans the raw data, filters out schedule-based predictions (which are less
reliable than GPS-based ones), and computes minutes-until-arrival from the
time the prediction was made.

Note on delay measurement: The CTA Train Tracker API does not provide a
scheduled time directly. We use the `is_delayed` flag from the API as our
primary on-time indicator. A more precise delay in minutes would require
joining to GTFS stop_times, but that join is complex (trip IDs aren't
returned by the real-time API). The mart models handle this approximation.
*/

with source as (
    select * from {{ source('raw', 'raw_train_arrivals') }}
),

cleaned as (
    select
        run_number,
        route,
        stop_id,
        station_id,
        station_name,
        stop_desc,
        dest_name,
        direction,
        predicted_arrival,
        prediction_made_at,
        collected_at,
        is_delayed,
        is_scheduled,
        is_fault,
        heading,

        -- Minutes from when prediction was made until predicted arrival
        datediff('minute', prediction_made_at, predicted_arrival) as minutes_away,

        -- Time dimensions for aggregation
        date_trunc('hour', prediction_made_at)  as prediction_hour,
        dayofweek(prediction_made_at)           as day_of_week,   -- 0=Sun, 6=Sat
        hour(prediction_made_at)                as hour_of_day,

        -- Friendly day name
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
    -- Exclude schedule-extrapolated predictions; they're less accurate
    where not is_scheduled
    -- Exclude fault flags (GPS/signal issues)
      and not is_fault
    -- Sanity check: only keep predictions within 90 minutes
      and minutes_away between 0 and 90
)

select * from cleaned
