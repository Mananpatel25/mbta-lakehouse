-- One row per route per hour: throughput, speed, and pipeline health.
select
    route_id,
    route_short_name,
    mode,
    event_hour,
    count(*)                                    as position_events,
    count(distinct vehicle_id)                  as active_vehicles,
    avg(speed_mph)                              as avg_speed_mph,
    quantile_cont(speed_mph, 0.5)               as p50_speed_mph,
    avg(ingest_lag_s)                           as avg_ingest_lag_s,
    max(ingest_lag_s)                           as max_ingest_lag_s,
    sum(case when current_status = 'STOPPED_AT' then 1 else 0 end) * 1.0 / count(*) as pct_stopped
from {{ ref('fct_vehicle_positions') }}
group by 1, 2, 3, 4
