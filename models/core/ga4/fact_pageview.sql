{{
  config(
    materialized='incremental',
    unique_key='event_id',
    partition_by={"field":"view_date","data_type":"date"},
    cluster_by=['user_key','ga_session_id','sk_page'],
    tags=['core','ga4','fact','content']
  )
}}

-- Grain: 1 row per page_view event (event_id)

with src as (
  select
    {{ dbt_utils.generate_surrogate_key(['user_pseudo_id','cast(ga_session_id as string)', 'page_location', 'page_referrer', 'page_title']) }} as sk_page,
    e.event_id,
    e.user_pseudo_id,
    e.ga_session_id,
    e.event_timestamp_utc,
    e.event_date_dt,    
    
    -- pageview params (ensure these are exposed in staging)
    {{ ga4_param_str('e.event_params','page_location') }} as page_location,
    {{ ga4_param_str('e.event_params','page_referrer') }} as page_referrer,
    {{ ga4_param_str('e.event_params','page_title') }}    as page_title


  -- FKs for device, geo, and traffic
    {{ fk_device_from_event('e') }} as device_key,
    {{ fk_geo_from_event('e') }} as geo_key,
    {{ fk_traffic_from_event('e') }} as traffic_key
    {{ page_key_expr('page_location') }} as page_key,
    {{ fk_date_key('e.event_date_dt') }} as date_key
    {{ session_key_expr('e.user_pseudo_id', 'e.ga_session_id') }} as session_key

  from {{ ref('stg_ga4_events') }} e
  where e.event_name = 'page_view'
  {% if is_incremental() %}
    and e.event_date_dt >= date_sub(current_date(), interval {{ var('core_lookback_days', 14) }} day)
  {% endif %}
),


enriched as (
  select
    s.*,

    -- window derivations (page order & time-on-page within the session)
    row_number() over (
      partition by s.user_pseudo_id, s.ga_session_id
      order by s.event_timestamp_utc asc, s.event_id asc
    ) as view_number_in_session,

     lead(s.event_timestamp_utc) over (
      partition by s.user_pseudo_id, s.ga_session_id
      order by s.event_timestamp_utc asc, s.event_id asc
    ) as next_pageview_ts,

    timestamp_diff(
      lead(s.event_timestamp_utc) over (
        partition by s.user_pseudo_id, s.ga_session_id
        order by s.event_timestamp_utc asc, s.event_id asc
      ),
      s.event_timestamp_utc,
      second
    ) as time_on_page_sec,

    -- time-on-site (cumulative across all sessions)
    sum(timestamp_diff(
      s.event_timestamp_utc,
      min(s.event_timestamp_utc) over (partition by s.user_pseudo_id),
      second
    )) over (partition by s.user_pseudo_id) as time_on_site_sec,


    -- entrance/exit flags
    case when row_number() over (
           partition by s.user_pseudo_id, s.ga_session_id
           order by s.event_timestamp_utc asc, s.event_id asc
         ) = 1 then true else false end as is_entrance,

    case when lead(s.event_timestamp_utc) over (
           partition by s.user_pseudo_id, s.ga_session_id
           order by s.event_timestamp_utc asc, s.event_id asc
         ) is null then true else false end as is_exit,
  from src s
),

select * from enriched