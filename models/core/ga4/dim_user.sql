
{{
  config(
    materialized='table',
    tags=['core','ga4','dim']
  )
}}

with latest as (
  select
    u.user_key,
    u.user_pseudo_id,
    u.first_seen_ts,
    u.last_seen_ts,
    u.device_category_latest,
    u.os_latest,
    u.country_latest,
    u.region_latest,
    u.city_latest
  from {{ ref('stg_ga4_users') }} u
),
ids as (
  select user_pseudo_id, any_value(user_id) as user_id
  from {{ ref('stg_ga4_events') }}
  where user_id is not null
  group by user_pseudo_id
)
select
  l.user_key,
  l.user_pseudo_id,
  i.user_id,
  l.first_seen_ts,
  l.last_seen_ts,
  date(l.first_seen_ts) as first_seen_date,
  date(l.last_seen_ts) as last_seen_date,
  l.device_category_latest,
  l.os_latest,
  l.country_latest,
  l.region_latest,
  l.city_latest
from latest l
left join ids i using (user_pseudo_id)