{{ 
  config(
    materialized='table', 
    tags=['core','ga4','dim','commerce'],
    unique_key='item_key',
    cluster_by=['item_key'],
  )
}}


-- Grain: 1 row per item_key (conformed with facts)

with base as (
  select
    i.item_key,
    any_value(nullif(i.item_name, ''))   as item_name_latest,
    any_value(nullif(i.item_variant, '')) as item_variant_latest,
    any_value(nullif(i.item_brand, ''))  as item_brand_latest,
    any_value(nullif(i.item_category, '')) as item_category,
    any_value(nullif(i.item_category2, '')) as item_category2,
    any_value(nullif(i.item_category3, '')) as item_category3,
    any_value(nullif(i.item_category4, '')) as item_category4,
    any_value(nullif(i.item_category5, '')) as item_category5,
    min(i.event_timestamp_utc) as first_seen_ts,
    max(i.event_timestamp_utc) as last_seen_ts,
    count(*) as observed_item_rows,
    -- basic price profiling
    min(i.item_price) as min_observed_price,
    max(i.item_price) as max_observed_price,
    avg(i.item_price) as avg_observed_price
  from {{ ref('stg_ga4_items') }} i
  where i.item_key is not null
  group by i.item_key
)


select
  item_key,
  item_name_latest,
  item_variant_latest,
  item_brand_latest,
  item_category,
  item_category2,
  item_category3,
  item_category4,
  item_category5,
  first_seen_ts,
  last_seen_ts,
  observed_item_rows,
  min_observed_price,
  max_observed_price,
  avg_observed_price
  (last_seen_ts >= timestamp_sub(current_timestamp(), interval {{ var('items_active_days', 180) }} day)) as is_active_recent
from base