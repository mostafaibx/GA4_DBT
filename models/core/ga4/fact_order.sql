{{
  config(
    materialized='incremental',
    unique_key='order_key',
    partition_by={"field":"order_date","data_type":"date"},
    cluster_by=['user_key','traffic_key', 'device_key', 'geo_key'],
    tags=['core','ga4','fact','commerce']
  )
}}

-- Grain: 1 row per purchase event (transaction_id)

with purchases as (
  select
    e.param_transaction_id as transaction_id,
    e.user_pseudo_id,
    e.ga_session_id,
    e.param_currency as currency,
    e.event_timestamp_utc,
    e.event_date_dt, 

    -- purchase-level amounts from the chosen event row
    coalesce(e.ecommerce_purchase_revenue, 0)      as revenue,
    coalesce(e.ecommerce_tax_value, 0)             as tax,
    coalesce(e.ecommerce_shipping_value, 0)        as shipping,

    -- device fields
    e.device_category,
    e.device_operating_system                      as device_os,
    e.device_operating_system_version              as device_os_version,
    e.device_web_browser                           as browser,
    e.device_web_browser_version                   as browser_version,

    -- geography fields  
    e.geo_country,
    e.geo_region,
    e.geo_city,
    coalesce(e.device_language, 'en')              as geo_language,

    -- traffic source fields
    e.traffic_source_source                        as traffic_source,
    e.traffic_source_medium                        as traffic_medium,
    {{ ga4_param_str('e.event_params', 'campaign') }}    as traffic_campaign,
    {{ ga4_param_str('e.event_params', 'content') }}     as traffic_content,
    {{ ga4_param_str('e.event_params', 'term') }}        as traffic_term,

    -- additional commerce parameters
    {{ ga4_param_str('e.event_params', 'coupon') }}      as order_coupon,
    {{ ga4_param_str('e.event_params', 'affiliation') }} as order_affiliation,
    {{ ga4_param_str('e.event_params', 'shipping_tier') }} as shipping_tier

  from {{ ref('stg_ga4_events') }} e
  where e.event_name = 'purchase'
    and e.param_transaction_id is not null
  {% if is_incremental() %}
    and e.event_date_dt >= date_sub(current_date(), interval {{ var('core_lookback_days', 14) }} day)
  {% endif %}
),



items_rollup as (
  -- Derive item counts from stg_ga4_items for validation & detailed KPIs
  select
    ev.param_transaction_id as transaction_id,
    count(*) as line_items,
    count(distinct it.product_item_id) as distinct_items,
    sum(it.item_quantity) as items_quantity,
    sum(it.item_revenue)  as items_revenue
  from {{ ref('stg_ga4_items') }} it
  join {{ ref('stg_ga4_events') }} ev
    on ev.event_id = it.event_id
  where ev.event_name = 'purchase'
    and ev.param_transaction_id is not null
  {% if is_incremental() %}
    and ev.event_date_dt >= date_sub(current_date(), interval {{ var('core_lookback_days', 14) }} day)
  {% endif %}
  group by 1
),

-- 3) Refunds (event-level amount; fall back to item-level if needed)
refund_events as (
  select
    ev.param_transaction_id                      as transaction_id,
    sum(coalesce(ev.ecommerce_refund_value, 0))  as refund_value_event
  from {{ ref('stg_ga4_events') }} ev
  where ev.event_name = 'refund'
    and ev.param_transaction_id is not null
  {% if is_incremental() %}
    and ev.event_date_dt >= date_sub(current_date(), interval {{ var('core_lookback_days', 14) }} day)
  {% endif %}
  group by 1
),


refund_items as (
  select
    ev.param_transaction_id                      as transaction_id,
    sum(coalesce(it.item_revenue, 0))            as refund_value_items,   -- depends on staging; refund rows often come as negative revenue
    sum(coalesce(it.item_quantity, 0))           as refunded_items_qty
  from {{ ref('stg_ga4_items') }} it
  join {{ ref('stg_ga4_events') }} ev
    on ev.event_id = it.event_id
  where ev.event_name = 'refund'
    and ev.param_transaction_id is not null
  {% if is_incremental() %}
    and ev.event_date_dt >= date_sub(current_date(), interval {{ var('core_lookback_days', 14) }} day)
  {% endif %}
  group by 1
),

refunds as (
  select
    coalesce(re.transaction_id, ri.transaction_id)                       as transaction_id,
    -- prefer explicit event total; else fall back to item-derived
    coalesce(re.refund_value_event, ri.refund_value_items, 0)            as refund_value,
    coalesce(ri.refunded_items_qty, 0)                                   as refunded_items_qty
  from refund_events re
  full outer join refund_items ri
    on re.transaction_id = ri.transaction_id
),



-- 4) Assemble fact with conformed keys and derived KPIs
assembled as (
  select
    -- surrogate keys
    {{ dbt_utils.generate_surrogate_key(['p.transaction_id']) }}         as order_key,
    {{ make_user_key('p.user_pseudo_id') }}                              as user_key,
    {{ make_session_key('p.user_pseudo_id','p.ga_session_id') }}         as session_key,
    {{ make_date_key('date(p.event_timestamp_utc)') }}                   as date_key,
    {{ make_device_key('p.device_category','p.device_os','p.device_os_version','p.browser','p.browser_version') }} as device_key,
    {{ make_geo_key('p.geo_country','p.geo_region','p.geo_city') }}                                as geo_key,
    {{ make_traffic_key('p.traffic_source','p.traffic_medium','p.traffic_campaign','p.traffic_content','p.traffic_term') }} as traffic_key,

    -- naturals + basics
    p.transaction_id,
    p.user_pseudo_id,
    p.ga_session_id,
    p.currency,
    p.event_timestamp_utc                                                as order_ts,
    date(p.event_timestamp_utc)                                          as order_date,

    -- amounts (purchase)
    p.revenue,
    p.tax,
    p.shipping,

    -- rollups
    ir.line_items,
    ir.distinct_items,
    ir.items_quantity,
    ir.items_revenue,

    -- refunds
    coalesce(rf.refund_value, 0)                                         as refund_value,
    coalesce(rf.refunded_items_qty, 0)                                   as refunded_items_qty,

    -- convenience metrics
    (p.revenue - coalesce(rf.refund_value, 0))                           as net_revenue,
    safe_divide(p.revenue, nullif(coalesce(ir.items_quantity), 0))       as avg_item_price,

    -- health checks / flags
    (abs(coalesce(ir.items_revenue, 0) - coalesce(p.revenue, 0)) > 0.01) as revenue_mismatch_flag,
    (coalesce(rf.refund_value, 0) > 0)                                   as has_refund_flag,

    -- purchase meta
    p.order_coupon,
    p.order_affiliation,
    p.shipping_tier

  from purchases p
  left join items_rollup ir
    on ir.transaction_id = p.transaction_id
  left join refunds rf
    on rf.transaction_id = p.transaction_id
)

select * from assembled