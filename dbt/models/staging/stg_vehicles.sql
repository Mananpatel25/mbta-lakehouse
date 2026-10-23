-- Type the raw payload, derive event timestamp, keep one row per event.
with src as (
    select * from {{ source('raw', 'vehicles') }}
),
typed as (
    select
        vehicle_id,
        label,
        route_id,
        trip_id,
        stop_id,
        direction_id,
        current_status,
        current_stop_sequence,
        latitude,
        longitude,
        bearing,
        speed * 2.23694                                   as speed_mph,   -- m/s → mph
        occupancy_status,
        cast(updated_at as timestamptz)                   as event_ts,
        cast(ingest_time as timestamptz)                  as ingest_ts,
        date_diff('second', cast(updated_at as timestamptz),
                            cast(ingest_time as timestamptz)) as ingest_lag_s,
        dt, hour
    from src
    where updated_at is not null
),
deduped as (
    select *,
        row_number() over (partition by vehicle_id, event_ts order by ingest_ts desc) as rn
    from typed
)
select * exclude (rn) from deduped where rn = 1
