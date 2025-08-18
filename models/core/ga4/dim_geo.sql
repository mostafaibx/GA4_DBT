{{
  config(
    materialized='table',
    tags=['core','ga4','dim']
  )
}}

-- Grain: one row per geo fingerprint = (country, region, city, language)

with base as (
  select
    coalesce(geo_continent,'')       as continent,
    coalesce(geo_sub_continent,'')   as sub_continent,
    coalesce(geo_country,'')         as country,
    coalesce(geo_region,'')          as region,
    coalesce(geo_metro,'')           as metro,
    coalesce(geo_city,'')            as city,
    event_timestamp_utc,
    user_pseudo_id
  from {{ ref('stg_ga4_events') }}
),
agg as (
  select
    continent, sub_continent, country, region, metro, city,
    min(event_timestamp_utc) as first_seen_ts,
    max(event_timestamp_utc) as last_seen_ts,
    count(distinct user_pseudo_id) as users_observed,
    count(*) as events_observed,
    count(distinct user_pseudo_id) as user_ids_observed
  from base
  group by 1,2,3,4,5,6
)
select
  {{ dbt_utils.generate_surrogate_key(["'ga4'","continent","sub_continent","country","region","metro","city"]) }} as geo_key,
  *
from agg