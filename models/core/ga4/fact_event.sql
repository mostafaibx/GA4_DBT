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
    {{ make_device_key('device_category','device_operating_system','device_os_version','browser','browser_version') }} as device_key,
    {{ make_geo_key('geo_country','geo_region','geo_city','geo_language') }}                                         as geo_key,
    {{ make_traffic_key('traffic_source','traffic_medium','traffic_campaign','traffic_content','traffic_term') }}     as traffic_key,

    -- Page key only when page attributes exist (often only for page_view)
    case 
      when page_host is not null or page_path is not null or page_title is not null then
        {{ make_page_key('page_host','page_path','page_title') }}
      else null
    end as page_key,

    -- Keep useful degenerate attributes for quick filters
    page_title,
    page_location,
    page_path,
    page_host,
    content_group,

    -- Common metrics-ready fields
    is_conversion,
    event_value_in_usd,
    engagement_time_msec,

    -- (Optional) carry session-scoped attribution fields at event time for convenience
    traffic_source,
    traffic_medium,
    traffic_campaign,
    traffic_content,
    traffic_term,

    -- (Optional) debugging columns
    device_category,
    device_operating_system as device_os,
    browser,
    geo_country,
    geo_region,
    geo_city

  from src
)

select * from keys
