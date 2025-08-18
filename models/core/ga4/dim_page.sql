{{ 
  config(
    materialized='table', 
    tags=['core','ga4','dim','content']
    ) 
}}

with pageviews as (
  select
    event_timestamp_utc,
    user_pseudo_id,
    ga_session_id,
    {{ ga4_param_str('event_params','page_location') }} as page_location,
    {{ ga4_param_str('event_params','page_title') }}    as page_title
  from {{ ref('stg_ga4_events') }}
  where event_name = 'page_view'
    and {{ ga4_param_str('event_params','page_location') }} is not null
),



canon as (
  select
    {{ page_key_expr('page_location') }}                   as page_key,
    lower(coalesce({{ url_host('page_location') }}, ''))   as page_domain,
    {{ url_path_normalized('page_location') }}             as page_path,
    {{ url_query_strip_tracking(url_query('page_location')) }} as page_query_clean,
    -- Latest observed title per page (by event timestamp)
    (select as struct * from (
      select page_title, event_timestamp_utc
      limit 1
    ) ).page_title as page_title_latest,
    min(event_timestamp_utc) as first_seen_ts,
    max(event_timestamp_utc) as last_seen_ts,
    count(*) as observed_views
  from pageviews
  group by 1,2,3,4,5
)
select
  page_key,
  page_domain,
  page_path,
  page_query_clean,
  page_title_latest,
  first_seen_ts,
  last_seen_ts,
  observed_views
from canon