{{
  config(
    materialized = 'table',
    tags = ['ga4','core','dim']
  )
}}

-- Grain: one row per traffic fingerprint

with base as (
  select
    lower(trim(coalesce(traffic_source_source,  ''))) as source,
    lower(trim(coalesce(traffic_source_name,  ''))) as source_name,
    lower(trim(coalesce(traffic_source_medium,  ''))) as medium,
    lower(trim(coalesce({{ ga4_param_str('event_params', 'campaign') }}, ''))) as campaign,
    lower(trim(coalesce({{ ga4_param_str('event_params', 'content') }}, ''))) as content,
    lower(trim(coalesce({{ ga4_param_str('event_params', 'term') }}, ''))) as term,
    event_timestamp_utc,
    user_pseudo_id,
    ga_session_id

  from {{ ref('stg_ga4_events') }}
  where ga_session_id is not null
),

agg as (
  select
    source, source_name, medium, campaign, content, term,


    min(event_timestamp_utc) as first_event_ts,
    max(event_timestamp_utc) as last_event_ts,
    count(distinct user_pseudo_id) as users_observed,
    count(*) as events_observed,
    count(distinct user_pseudo_id) as user_ids_observed
  from base
  group by 1,2,3,4,5,6
),

classified as (
  -- derive a stable channel grouping from source/medium/term (independent of GA4 label)
  select
    a.*,

    -- extract domain-ish tokens from source for classification convenience
    -- (if source looks like a URL or domain, pull host; else keep as-is)
    coalesce(nullif(regexp_extract(a.source, r'^(?:https?://)?([^/]+)'), ''), a.source) as source_host,

    -- boolean helpers
    REGEXP_CONTAINS(a.medium, r'(cpc|ppc|paidsearch|paid-search|sem)') as is_paid_search_medium,
    REGEXP_CONTAINS(a.medium, r'(paid[_\- ]?social|paidsocial)')       as is_paid_social_medium,
    REGEXP_CONTAINS(a.medium, r'(display|banner|cpm)')                 as is_display_medium,
    REGEXP_CONTAINS(a.medium, r'(video|cpv|trueview)')                 as is_video_medium,
    REGEXP_CONTAINS(a.medium, r'(email|e-mail|newsletter)')            as is_email_medium,
    REGEXP_CONTAINS(a.medium, r'(affiliate)')                          as is_affiliate_medium,
    REGEXP_CONTAINS(a.medium, r'(organic|seo)')                        as is_organic_medium,
    REGEXP_CONTAINS(a.medium, r'(referral)')                           as is_referral_medium,
    (a.medium = '(none)' or a.source in ('(direct)','direct','')) as is_direct_hint,

    -- basic social source list (extend as needed)
    (a.source in ('facebook','fb','instagram','ig','tiktok','twitter','x','linkedin','pinterest',
                  'youtube','yt','snapchat','reddit','quora','threads','wechat','weibo','vk','line')) as is_social_source,

    -- final derived channel grouping (you can tune these rules)
    case
      when (a.medium = '(none)' or a.source in ('(direct)','direct','')) and a.campaign = '' and a.term = '' then 'Direct'
      when REGEXP_CONTAINS(a.medium, r'(paid[_\- ]?social|paidsocial)') or (a.source in ('facebook','fb','instagram','ig','tiktok','twitter','x','linkedin','pinterest','youtube','yt','snapchat','reddit','quora','threads','wechat','weibo','vk','line') and REGEXP_CONTAINS(a.medium, r'(display|banner|cpm)')) then 'Paid Social'
      when a.source in ('facebook','fb','instagram','ig','tiktok','twitter','x','linkedin','pinterest','youtube','yt','snapchat','reddit','quora','threads','wechat','weibo','vk','line') or a.medium in ('social','social-network','social-media','sm') then 'Organic Social'
      when REGEXP_CONTAINS(a.medium, r'(cpc|ppc|paidsearch|paid-search|sem)') or (a.medium in ('cpc','ppc') or (a.source in ('google','bing','yahoo','baidu','duckduckgo') and a.medium in ('paid','cpc','ppc')))
           then 'Paid Search'
      when REGEXP_CONTAINS(a.medium, r'(organic|seo)') or (a.medium = 'organic') or (a.source in ('google','bing','yahoo','baidu','duckduckgo') and a.medium in ('organic','(organic)'))
           then 'Organic Search'
      when REGEXP_CONTAINS(a.medium, r'(display|banner|cpm)') then 'Display'
      when REGEXP_CONTAINS(a.medium, r'(video|cpv|trueview)') then 'Video'
      when REGEXP_CONTAINS(a.medium, r'(email|e-mail|newsletter)') then 'Email'
      when REGEXP_CONTAINS(a.medium, r'(affiliate)') then 'Affiliates'
      when REGEXP_CONTAINS(a.medium, r'(referral)') or a.medium = 'referral' or (coalesce(nullif(regexp_extract(a.source, r'^(?:https?://)?([^/]+)'), ''), a.source) not in ('','(direct)','direct') and a.campaign = '' and a.medium = '')
           then 'Referral'
      else 'Unassigned'
    end as channel_grouping

  from agg a
),

final as (
  select
    {{ make_traffic_key('source','medium','campaign','content','term') }} as traffic_key,

    -- canonical key components (lowercased)
    source, medium, campaign, content, term,

    -- display/casing-friendly variants
    nullif(initcap(source),   '') as source_display,
    nullif(lower(medium),     '') as medium_display,   -- mediums are conventional in lower
    nullif(initcap(campaign), '') as campaign_display,
    nullif(initcap(content),  '') as content_display,
    nullif(lower(term),       '') as term_display,

    -- derived attributes
    channel_grouping,
    nullif(initcap(source_host), '') as source_host,

    -- paid/owned flags (helpful for quick filters)
    case
      when channel_grouping in ('Paid Search','Paid Social','Display','Video') then true else false end as is_paid,
    case
      when channel_grouping in ('Organic Search','Organic Social','Referral','Email','Direct') then true else false end as is_nonpaid,

    -- activity metadata
    first_event_ts as first_seen_ts,
    last_event_ts as last_seen_ts,
    date(first_event_ts) as first_seen_date,
    date(last_event_ts)  as last_seen_date,
    events_observed,
    users_observed
  from classified
)

select * from final

