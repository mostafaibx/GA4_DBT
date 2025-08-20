{{
  config(
    materialized='incremental',
    unique_key='event_id',
    partition_by={"field":"event_date_dt","data_type":"date"},
    cluster_by=['user_key','ga_session_id','sk_page'],
    tags=['core','ga4','fact','content']
  )
}}

-- Grain: 1 row per page_view event (event_id)

with base_events as (
  select
    e.event_id,
    e.user_pseudo_id,
    e.ga_session_id,
    e.event_timestamp_utc,
    e.event_date_dt,
    
    -- pageview params (ensure these are exposed in staging)
    {{ ga4_param_str('e.event_params','page_location') }} as page_location,
    {{ ga4_param_str('e.event_params','page_referrer') }} as page_referrer,
    {{ ga4_param_str('e.event_params','page_title') }}    as page_title,
    
    -- traffic source params for FK generation
    {{ ga4_param_str('e.event_params','campaign') }}      as traffic_campaign,
    {{ ga4_param_str('e.event_params','content') }}       as traffic_content,
    {{ ga4_param_str('e.event_params','term') }}          as traffic_term,
    
    -- device fields
    e.device_category,
    e.device_operating_system,
    e.device_operating_system_version,
    e.device_web_browser,
    e.device_web_browser_version,
    
    -- geography fields
    e.geo_country,
    e.geo_region,
    e.geo_city,
    e.device_language,
    
    -- traffic source fields
    e.traffic_source_source,
    e.traffic_source_medium

  from {{ ref('stg_ga4_events') }} e
  where e.event_name = 'page_view'
  {% if is_incremental() %}
    and e.event_date_dt >= date_sub(current_date(), interval {{ var('core_lookback_days', 14) }} day)
  {% endif %}
),

src as (
  select
    *,
    -- generate surrogate key with extracted page params
    {{ dbt_utils.generate_surrogate_key(['user_pseudo_id','cast(ga_session_id as string)', 'page_location', 'page_referrer', 'page_title']) }} as sk_page,
    
    -- FKs for device, geo, and traffic  
    {{ make_user_key('user_pseudo_id') }} as user_key,
    {{ make_device_key('device_category','device_operating_system','device_operating_system_version','device_web_browser','device_web_browser_version') }} as device_key,
    {{ make_geo_key('geo_country','geo_region','geo_city') }} as geo_key,
    {{ make_traffic_key('traffic_source_source','traffic_source_medium','traffic_campaign','traffic_content','traffic_term') }} as traffic_key,
    {{ make_page_key('page_location') }} as page_key,
    {{ make_date_key('event_date_dt') }} as date_key,
    {{ make_session_key('user_pseudo_id', 'ga_session_id') }} as session_key
    
  from base_events
),


with_window_functions as (
  select
    s.*,

    -- basic window derivations (page order & next pageview)
    row_number() over (
      partition by s.user_pseudo_id, s.ga_session_id
      order by s.event_timestamp_utc asc, s.event_id asc
    ) as view_number_in_session,

    lead(s.event_timestamp_utc) over (
      partition by s.user_pseudo_id, s.ga_session_id
      order by s.event_timestamp_utc asc, s.event_id asc
    ) as next_pageview_ts,

    -- get min timestamp per user for time-on-site calculation
    min(s.event_timestamp_utc) over (partition by s.user_pseudo_id) as user_first_pageview_ts

  from src s
),

enriched as (
  select
    *,

    -- calculate time on page using pre-calculated next_pageview_ts
    timestamp_diff(next_pageview_ts, event_timestamp_utc, second) as time_on_page_sec,

    -- calculate time from first pageview for this specific pageview
    timestamp_diff(event_timestamp_utc, user_first_pageview_ts, second) as time_from_first_pageview_sec,

    -- entrance/exit flags
    case when view_number_in_session = 1 then true else false end as is_entrance,
    case when next_pageview_ts is null then true else false end as is_exit

  from with_window_functions
)

select * from enriched