select
    route_id,
    route_short_name,
    route_long_name,
    route_type,
    route_color,
    route_desc,
    cast(snapshot_date as date) as snapshot_date
from {{ source('raw', 'gtfs_routes') }}
qualify row_number() over (partition by route_id order by snapshot_date desc) = 1
