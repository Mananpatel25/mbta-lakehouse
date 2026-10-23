{% snapshot routes_snapshot %}
{{ config(
    target_schema='snapshots',
    unique_key='route_id',
    strategy='check',
    check_cols=['route_short_name', 'route_long_name', 'route_type', 'route_color', 'route_desc']
) }}
-- SCD Type 2: dbt adds dbt_valid_from / dbt_valid_to. A route rename or recolor
-- closes the old row and opens a new one; history is never overwritten.
select * from {{ source('raw', 'gtfs_routes') }}
qualify row_number() over (partition by route_id order by snapshot_date desc) = 1
{% endsnapshot %}
