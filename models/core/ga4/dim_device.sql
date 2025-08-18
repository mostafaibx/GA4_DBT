{{
  config(
    materialized='table',
    tags=['core','ga4','dim']
  )
}}

-- Grain: one row per device fingerprint 


with base as (
  select
    lower(coalesce(device_category,''))                 as device_category,
    lower(coalesce(device_operating_system,''))         as os,
    lower(coalesce(device_operating_system_version,'')) as os_version,
    lower(coalesce(device_web_browser,''))              as browser,
    lower(coalesce(device_web_browser_version,''))      as browser_version,
    lower(coalesce(device_mobile_brand_name,''))        as brand,
    lower(coalesce(device_mobile_model_name,''))        as model,
    lower(coalesce(device_mobile_marketing_name,''))    as marketing_name,
    coalesce(device_mobile_os_hardware_model, null)     as hardware_model,
    coalesce(device_language, '')                       as language,
    coalesce(cast(device_is_limited_ad_tracking as bool), false) as limit_ad_tracking,
    event_timestamp_utc,
    user_pseudo_id
  from {{ ref('stg_ga4_events') }}
),


    -- Group only by the fingerprint columns that define the conformed device_key
agg as (


  select
    device_category, os, os_version, browser, browser_version,
    brand, model, marketing_name, hardware_model, language, limit_ad_tracking,
    min(event_timestamp_utc) as first_seen_ts,
    max(event_timestamp_utc) as last_seen_ts,
    count(*)                 as events_observed,
    count(distinct user_pseudo_id) as users_observed
  from base
  group by 1,2,3,4,5,6,7,8,9,10,11
)


select
  {{ make_device_key('device_category','os','os_version','browser','browser_version') }} as device_key,
 case
      when browser like '%edge%' then 'edge'
      when browser like '%firefox%' then 'firefox'
      when browser like '%samsung%' then 'samsung internet'
      when browser like '%opera%' then 'opera'
      when browser like '%safari%' and browser not like '%chrome%' then 'safari'
      when browser like '%chrome%' then 'chrome'
      else browser
    end as browser_family,

    case when device_category in ('mobile','tablet') then true else false end as is_mobile,
    case when device_category = 'tablet' then true else false end             as is_tablet,
    case when device_category = 'desktop' then true else false end            as is_desktop,
  *
from agg