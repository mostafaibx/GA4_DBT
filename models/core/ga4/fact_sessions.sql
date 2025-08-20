{{
  config(
    materialized = 'incremental',
    unique_key   = 'session_key',
    partition_by = {"field": "session_start_date", "data_type": "date"},
    cluster_by   = ["user_key","traffic_key"],
    tags         = ['ga4','fact']
  )
}}

with events as (
  select *
  from {{ ref('stg_ga4_events') }}
  where ga_session_id is not null
  {% if is_incremental() %}
    and event_date_dt >= date_sub(current_date(), interval {{ var('ga4_incremental_lookback_days') }} day)
  {% endif %}
),


-- First event in the session (for device/geo/traffic & session start attribution)
first_event as (
  select
    user_pseudo_id,
    ga_session_id,
    -- earliest event timestamp in the session
    min(event_timestamp_utc) as session_start_ts,

    -- pick "first" values deterministically by time
    (array_agg(device_category           order by event_timestamp_utc asc limit 1))[offset(0)] as device_category,
    (array_agg(device_operating_system   order by event_timestamp_utc asc limit 1))[offset(0)] as device_os,
    (array_agg(device_operating_system_version         order by event_timestamp_utc asc limit 1))[offset(0)] as device_os_version,
    (array_agg(device_web_browser                   order by event_timestamp_utc asc limit 1))[offset(0)] as browser,
    (array_agg(device_web_browser_version           order by event_timestamp_utc asc limit 1))[offset(0)] as browser_version,

    (array_agg(geo_country               order by event_timestamp_utc asc limit 1))[offset(0)] as geo_country,
    (array_agg(geo_region                order by event_timestamp_utc asc limit 1))[offset(0)] as geo_region,
    (array_agg(geo_city                  order by event_timestamp_utc asc limit 1))[offset(0)] as geo_city,

    -- prefer values from the first event; adapt if you expose session_* fields in staging
    (array_agg(traffic_source_name            order by event_timestamp_utc asc limit 1))[offset(0)] as traffic_source,
    (array_agg(traffic_source_medium            order by event_timestamp_utc asc limit 1))[offset(0)] as traffic_medium,
    (array_agg(traffic_campaign          order by event_timestamp_utc asc limit 1))[offset(0)] as traffic_campaign,
    (array_agg(traffic_content           order by event_timestamp_utc asc limit 1))[offset(0)] as traffic_content,
    (array_agg(traffic_term              order by event_timestamp_utc asc limit 1))[offset(0)] as traffic_term,

    -- session number if available in staging (e.g., ga_session_number)
    max(ga_session_number) as session_number

  from events
  group by 1,2
),


-- First page_view in the session (landing page)
first_page as (
  select
    user_pseudo_id,
    ga_session_id,
    (array_agg({{ ga4_param_str('event_params', 'page_location') }} order by event_timestamp_utc asc limit 1))[offset(0)] as lp_location,
    (array_agg({{ ga4_param_str('event_params', 'page_title') }}    order by event_timestamp_utc asc limit 1))[offset(0)] as lp_title,
    (array_agg({{ ga4_param_str('event_params', 'page_path') }}     order by event_timestamp_utc asc limit 1))[offset(0)] as lp_path,
    -- Extract host from page_location using regexp
    (array_agg(
      regexp_extract({{ ga4_param_str('event_params', 'page_location') }}, r'https?://([^/]+)')
      order by event_timestamp_utc asc limit 1))[offset(0)] as lp_host
  from events
  where event_name = 'page_view'
  group by 1,2
),


-- Session rollups (metrics)
rollups as (
  select
    user_pseudo_id,
    ga_session_id,

    min(event_timestamp_utc) as session_start_ts,
    max(event_timestamp_utc) as session_end_ts,

    count(*) as events,
    countif(event_name = 'page_view') as pageviews,
    sum( {{ ga4_param_int('event_params','engagement_time_msec') }} ) as engagement_time_ms,

    -- conversion count based on conversion event names
    countif(event_name in ('purchase', 'generate_lead', 'sign_up')) as conversions,

    -- dedupe transactions & revenue only on purchase events
    count(distinct case when event_name = 'purchase' then nullif(param_transaction_id, '') end) as transactions,
    sum(case when event_name = 'purchase' then coalesce(ecommerce_purchase_revenue, 0) else 0 end) as revenue
  from events
  group by 1,2
),


assembled as (
  select
    -- Surrogate keys (consistent with your FK macros)
    {{ make_session_key('e.user_pseudo_id','e.ga_session_id') }} as session_key,
    {{ make_user_key('e.user_pseudo_id') }}                      as user_key,

    {{ make_device_key('fe.device_category','fe.device_os','fe.device_os_version','fe.browser','fe.browser_version') }} as device_key,
    {{ make_geo_key('fe.geo_country','fe.geo_region','fe.geo_city') }}                                as geo_key,
    {{ make_traffic_key('fe.traffic_source','fe.traffic_medium','fe.traffic_campaign','fe.traffic_content','fe.traffic_term') }} as traffic_key,

    -- landing page key (nullable)
    case when fp.lp_location is not null then
      {{ make_page_key('fp.lp_location') }}
    else null end as landing_page_key,

    -- natural identifiers
    e.user_pseudo_id,
    e.ga_session_id,

    -- timing
    r.session_start_ts,
    r.session_end_ts,
    timestamp_diff(r.session_end_ts, r.session_start_ts, second) as session_duration_sec,
    date(r.session_start_ts) as session_start_date,
    {{ make_date_key('date(r.session_start_ts)') }}               as date_key,

    -- metrics
    r.events,
    r.pageviews,
    r.engagement_time_ms,
    r.conversions,
    r.transactions,
    r.revenue,

    -- engagement (GA4 definition: ≥10s OR >=2 pageviews/screenviews OR any conversion)
    (r.engagement_time_ms >= 10000 or r.pageviews >= 2 or r.conversions > 0) as engaged_session_flag,

    -- attribution/context at session start
    fe.session_number,
    fe.device_category,
    fe.device_os,
    fe.device_os_version,
    fe.browser,
    fe.browser_version,
    fe.geo_country,
    fe.geo_region,
    fe.geo_city,
    fe.traffic_source,
    fe.traffic_medium,
    fe.traffic_campaign,
    fe.traffic_content,
    fe.traffic_term,

    -- landing page attributes (for convenience)
    fp.lp_host,
    fp.lp_path,
    fp.lp_title

  from rollups r
  join first_event fe
    on fe.user_pseudo_id = r.user_pseudo_id
   and fe.ga_session_id  = r.ga_session_id
  left join first_page fp
    on fp.user_pseudo_id = r.user_pseudo_id
   and fp.ga_session_id  = r.ga_session_id
  join events e
    on e.user_pseudo_id = r.user_pseudo_id
   and e.ga_session_id  = r.ga_session_id
  -- e is only used for key generation inputs; dedupe with select distinct
  qualify row_number() over (partition by r.user_pseudo_id, r.ga_session_id order by r.session_start_ts) = 1
)

select * from assembled