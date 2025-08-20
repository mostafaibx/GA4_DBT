{{ 
  config(
    materialized = 'incremental',
    unique_key   = 'event_id',
    on_schema_change = 'sync_all_columns',
    partition_by = {"field": "event_date_dt", "data_type": "date"},
    cluster_by   = ["event_name","session_key","user_key"],
    tags         = ['ga4','fact']
  ) 
}}

-- Grain: 1 row per GA4 event (event_id)

with src as (
  select *
  from {{ ref('stg_ga4_events') }}
  {% if is_incremental() %}
    -- Reprocess trailing days to catch late-arriving events (default 3)
    where event_date_dt >= date_sub(current_date(), interval {{ var('ga4_late_arrival_days', 3) }} day)
  {% endif %}
), 

enriched as (
  select
    *,
    -- Extract page location for page key
    {{ ga4_param_str('event_params', 'page_location') }} as page_location_param
  from src
),

keys as (
  select
    -- Natural identifiers
    event_id,
    event_name,
    event_date_dt,
    event_timestamp_utc,
    event_previous_timestamp_utc,
    user_pseudo_id,
    ga_session_id,
 
    -- Foreign keys (deterministic, conformed across dims)
    {{ make_date_key('event_date_dt') }}                                   as date_key,
    {{ make_user_key('user_pseudo_id') }}                                  as user_key,
    {{ make_session_key('user_pseudo_id','ga_session_id') }}               as session_key,
    {{ make_device_key('device_category','device_operating_system','device_operating_system_version','device_web_browser','device_web_browser_version') }} as device_key,
    {{ make_geo_key('geo_country','geo_region','geo_city') }}                                         as geo_key,
    {{ make_traffic_key('traffic_source_source','traffic_source_medium','traffic_campaign','traffic_content','traffic_term') }}     as traffic_key,

    -- Page key - use extracted page location
    case 
      when page_location_param is not null then
        {{ make_page_key('page_location_param') }}
      else null
    end as page_key,

    -- Keep useful degenerate attributes for quick filters
    {{ ga4_param_str('event_params', 'page_title') }} as page_title,
    {{ ga4_param_str('event_params', 'page_location') }} as page_location,
    {{ ga4_param_str('event_params', 'page_path') }} as page_path,
    {{ ga4_param_str('event_params', 'page_referrer') }} as page_referrer,
    {{ ga4_param_str('event_params', 'content_group1') }} as content_group,

    -- Common metrics-ready fields
    case when event_name in ('purchase', 'generate_lead', 'sign_up') then true else false end as is_conversion,
    event_value_in_usd,
    cast({{ ga4_param_int('event_params', 'engagement_time_msec') }} as int64) as engagement_time_msec,

    -- (Optional) carry session-scoped attribution fields at event time for convenience
    traffic_source_source as traffic_source,
    traffic_source_medium as traffic_medium,
    traffic_source_name as traffic_campaign,

    -- (Optional) debugging columns
    device_category,
    device_operating_system as device_os,
    device_web_browser as browser,
    geo_country,
    geo_region,
    geo_city

  from enriched
)

select * from keys
