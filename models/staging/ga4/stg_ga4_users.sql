{{ config(materialized='view') }}

with ranked as (
  select
    user_pseudo_id,
    device_category,
    device_operating_system,
    geo_country,
    geo_region,
    geo_city,
    event_timestamp_utc,
    row_number() over (
      partition by user_pseudo_id
      order by event_timestamp_utc desc
    ) as rn
  from {{ ref('stg_ga4_events') }}
)
select
  {{ dbt_utils.generate_surrogate_key(['user_pseudo_id']) }} as user_key,
  user_pseudo_id,
  event_timestamp_utc as last_seen_ts,
  -- first_seen_ts via a separate MIN() aggregation
  (select min(event_timestamp_utc) from {{ ref('stg_ga4_events') }} e
    where e.user_pseudo_id = ranked.user_pseudo_id) as first_seen_ts,
  device_category        as device_category_latest,
  device_operating_system as os_latest,
  geo_country            as country_latest,
  geo_region             as region_latest,
  geo_city               as city_latest
from ranked
where rn = 1