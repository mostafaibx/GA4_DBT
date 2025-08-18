{{
  config(
    materialized = 'table',
    tags = ['ga4','core','dim']
  )
}}

-- Grain: one row per traffic fingerprint

with base as (
  select
    lower(trim(coalesce(traffic_source,  ''))) as source,
    lower(trim(coalesce(traffic_medium,  ''))) as medium,
    lower(trim(coalesce(traffic_campaign,''))) as campaign,
    lower(trim(coalesce(traffic_content, ''))) as content,
    lower(trim(coalesce(traffic_term,    ''))) as term,
    event_timestamp_utc,
    user_pseudo_id,
    ga_session_id

  from {{ ref('stg_ga4_events') }}
  where ga_session_id is not null
),

agg as (
  select
    source, medium, campaign, content, term,


    min(event_timestamp_utc) as first_event_ts,
    max(event_timestamp_utc) as last_event_ts,
    count(distinct user_pseudo_id) as users_observed,
    count(*) as events_observed,
    count(distinct user_pseudo_id) as user_ids_observed
  from base
  group by 1,2,3,4,5
),

classified as (
  -- derive a stable channel grouping from source/medium/term (independent of GA4 label)
  select
    a.*,

    -- extract domain-ish tokens from source for classification convenience
    -- (if source looks like a URL or domain, pull host; else keep as-is)
    coalesce(nullif(regexp_extract(a.source, r'^(?:https?://)?([^/]+)'), ''), a.source) as source_host,

    -- boolean helpers
    (a.medium regexp r'(cpc|ppc|paidsearch|paid-search|sem)') as is_paid_search_medium,
    (a.medium regexp r'(paid[_\- ]?social|paidsocial)')       as is_paid_social_medium,
    (a.medium regexp r'(display|banner|cpm)')                 as is_display_medium,
    (a.medium regexp r'(video|cpv|trueview)')                 as is_video_medium,
    (a.medium regexp r'(email|e-mail|newsletter)')            as is_email_medium,
    (a.medium regexp r'(affiliate)')                          as is_affiliate_medium,
    (a.medium regexp r'(organic|seo)')                        as is_organic_medium,
    (a.medium regexp r'(referral)')                           as is_referral_medium,
    (a.medium = '(none)' or a.source in ('(direct)','direct','')) as is_direct_hint,

    -- basic social source list (extend as needed)
    (a.source in ('facebook','fb','instagram','ig','tiktok','twitter','x','linkedin','pinterest',
                  'youtube','yt','snapchat','reddit','quora','threads','wechat','weibo','vk','line')) as is_social_source,

    -- final derived channel grouping (you can tune these rules)
    case
      when is_direct_hint and a.campaign = '' and a.term = '' then 'Direct'
      when is_paid_social_medium or (is_social_source and is_display_medium) then 'Paid Social'
      when is_social_source or a.medium in ('social','social-network','social-media','sm') then 'Organic Social'
      when is_paid_search_medium or (a.medium in ('cpc','ppc') or (a.source in ('google','bing','yahoo','baidu','duckduckgo') and a.medium in ('paid','cpc','ppc')))
           then 'Paid Search'
      when is_organic_medium or (a.medium = 'organic') or (a.source in ('google','bing','yahoo','baidu','duckduckgo') and a.medium in ('organic','(organic)'))
           then 'Organic Search'
      when is_display_medium then 'Display'
      when is_video_medium then 'Video'
      when is_email_medium then 'Email'
      when is_affiliate_medium then 'Affiliates'
      when is_referral_medium or a.medium = 'referral' or (a.source_host not in ('','(direct)','direct') and a.campaign = '' and a.medium = '')
           then 'Referral'
      else 'Unassigned'
    end as channel_grouping,

    -- paid/owned flags (helpful for quick filters)
    case
      when channel_grouping in ('Paid Search','Paid Social','Display','Video') then true else false end as is_paid,
    case
      when channel_grouping in ('Organic Search','Organic Social','Referral','Email','Direct') then true else false end as is_nonpaid

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
    ga4_channel_grouping,            -- GA4's own label, if available

    is_paid,
    is_nonpaid,

    -- activity metadata
    first_seen_ts,
    last_seen_ts,
    date(first_seen_ts) as first_seen_date,
    date(last_seen_ts)  as last_seen_date,
    events_observed,
    sessions_observed,
    users_observed
  from classified
)

select * from final

