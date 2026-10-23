{{ config(
    materialized='incremental',
    unique_key=['vehicle_id', 'event_ts'],
    incremental_strategy='delete+insert'
) }}

-- Incremental with a 2-hour lookback: late-arriving events (ingest_lag > 0)
-- for recent hours get picked up on the next run instead of being dropped.
with v as (
    select * from {{ ref('stg_vehicles') }}
    {% if is_incremental() %}
    where event_ts >= (select coalesce(max(event_ts), '1970-01-01'::timestamptz) - interval 2 hour from {{ this }})
    {% endif %}
),
r as (
    select route_id, route_short_name, route_type from {{ ref('stg_routes') }}
)
select
    v.vehicle_id,
    v.event_ts,
    v.ingest_ts,
    v.ingest_lag_s,
    v.route_id,
    r.route_short_name,
    case r.route_type
        when 0 then 'light_rail' when 1 then 'subway' when 2 then 'commuter_rail'
        when 3 then 'bus' when 4 then 'ferry' else 'other' end as mode,
    v.trip_id,
    v.stop_id,
    v.direction_id,
    v.current_status,
    v.latitude,
    v.longitude,
    v.bearing,
    v.speed_mph,
    v.occupancy_status,
    date_trunc('hour', v.event_ts) as event_hour
from v
left join r using (route_id)
