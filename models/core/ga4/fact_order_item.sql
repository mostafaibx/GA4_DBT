{{
  config(
    materialized='incremental',
    unique_key='order_item_key',
    partition_by={"field":"order_date","data_type":"date"},
    cluster_by=['user_key','ga_session_id','item_key'],
    tags=['core','ga4','fact','commerce']
  )
}}

-- Grain: one row per purchased line item (transaction_id + item_index)

with base as (
  select
    ev.param_transaction_id as transaction_id,
    ev.user_pseudo_id,
    ev.ga_session_id,
    ev.param_currency as currency,
    ev.event_timestamp_utc as order_ts,
    it.event_id,
    it.item_index,

    -- item identifiers
    it.item_id,
    it.item_key,
    it.item_name,
    it.item_brand,
    it.item_variant,
    it.item_category,
    it.item_category2,
    it.item_category3,
    it.item_category4,
    it.item_category5,

    -- item-level amounts
    it.item_price,
    it.item_quantity,
    it.item_revenue,
    it.item_refund,
    it.item_coupon,
    it.item_affiliation,

    -- context used for conformed keys (optional joins avoided later)
    ev.device_category,
    ev.device_operating_system                     as device_os,
    ev.device_operating_system_version             as device_os_version,
    ev.device_web_browser                          as browser,
    ev.device_web_browser_version                  as browser_version,
    ev.geo_country,
    ev.geo_region,
    ev.geo_city,
    ev.traffic_source_source                       as traffic_source,
    ev.traffic_source_medium                       as traffic_medium,
    ev.traffic_source_name                         as traffic_campaign,
    ev.event_date_dt,
    {{ ga4_param_str('ev.event_params', 'page_location') }} as page_location




  from {{ ref('stg_ga4_items') }} it
  join {{ ref('stg_ga4_events') }} ev
    on ev.event_id = it.event_id
  where ev.event_name = 'purchase'
    and ev.param_transaction_id is not null
  {% if is_incremental() %}
    and ev.event_date_dt >= date_sub(current_date(), interval {{ var('core_lookback_days', 14) }} day)
  {% endif %}
),


-- 2) Deduplicate in the rare case the same (transaction_id, item_index) appears twice
deduped as (
  select * except(rn)
  from (
    select b.*,
           row_number() over (
             partition by b.transaction_id, b.item_index
             order by b.order_ts desc, b.event_id desc
           ) as rn
    from base b
  )
  where rn = 1
),

-- 3) Refund items (item-level refunds). Some properties send item rows with refund events.
refund_items as (
  select
    ev.param_transaction_id                        as transaction_id,
    it.item_key,
    it.item_id as product_item_id,
    sum(coalesce(it.item_quantity, 0))             as refunded_item_quantity,
    sum(coalesce(it.item_revenue, 0))              as refunded_item_revenue  -- often negative in staging; we will subtract
  from {{ ref('stg_ga4_items') }} it
  join {{ ref('stg_ga4_events') }} ev
    on ev.event_id = it.event_id
  where ev.event_name = 'refund'
    and ev.param_transaction_id is not null
  {% if is_incremental() %}
    and ev.event_date_dt >= date_sub(current_date(), interval {{ var('core_lookback_days', 14) }} day)
  {% endif %}
  group by 1,2,3
),




-- 4) Assemble line items with conformed keys + refund rollups
assembled as (
  select
    -- surrogate keys
    {{ dbt_utils.generate_surrogate_key([
      'transaction_id',
      "cast(item_index as string)"
    ]) }}                                                                as order_item_key,
    {{ dbt_utils.generate_surrogate_key(['transaction_id']) }}           as order_key,          -- join to fact_order quickly

    -- conformed FKs
    {{ make_user_key('d.user_pseudo_id') }}                            as user_key,
    {{ make_session_key('d.user_pseudo_id','d.ga_session_id') }}     as session_key,
    {{ make_date_key('date(d.order_ts)') }}                            as date_key,
    {{ make_device_key('d.device_category','d.device_os','d.device_os_version','d.browser','d.browser_version') }} as device_key,
    {{ make_geo_key('d.geo_country','d.geo_region','d.geo_city') }}                                   as geo_key,
    {{ make_traffic_key('d.traffic_source','d.traffic_medium','d.traffic_campaign','cast(null as string)','cast(null as string)') }} as traffic_key,

    -- natural identifiers & timing
    d.transaction_id,
    d.user_pseudo_id,
    d.ga_session_id,
    d.currency,
    d.order_ts,
    date(d.order_ts)                                                   as order_date,

    -- product keys + canonical attributes from the dimension (if present)
    d.item_key,
    coalesce(di.item_id, d.item_id)                    as product_item_id,
    coalesce(di.item_name,       d.item_name)                          as item_name,
    coalesce(di.item_brand,      d.item_brand)                         as item_brand,
    coalesce(di.item_variant,    d.item_variant)                       as item_variant,
    coalesce(di.item_category,   d.item_category)                      as item_category1,
    coalesce(di.item_category2,  d.item_category2)                     as item_category2,
    coalesce(di.item_category3,  d.item_category3)                     as item_category3,
    coalesce(di.item_category4,  d.item_category4)                     as item_category4,
    coalesce(di.item_category5,  d.item_category5)                     as item_category5,

    -- item economics (purchase)
    d.item_index,
    d.item_price,
    d.item_quantity,
    d.item_revenue,

    -- refund economics (item level)
    coalesce(ri.refunded_item_quantity, 0)                               as refunded_item_quantity,
    coalesce(ri.refunded_item_revenue, 0)                                as refunded_item_revenue,

    -- net amounts (purchase minus refunds)
    greatest(d.item_quantity - coalesce(ri.refunded_item_quantity, 0), 0) as net_item_quantity,
    (d.item_revenue - coalesce(ri.refunded_item_revenue, 0))              as net_item_revenue,

    -- derived KPIs
    greatest(coalesce(d.item_price, 0) * coalesce(d.item_quantity, 0) - coalesce(d.item_revenue, 0), 0) as item_discount_value,
    safe_divide(d.item_revenue, nullif(d.item_quantity, 0))          as purchase_unit_price,
    safe_divide((d.item_revenue - coalesce(ri.refunded_item_revenue, 0)),
                nullif(greatest(d.item_quantity - coalesce(ri.refunded_item_quantity, 0), 0), 0))          as effective_unit_price,

    -- optional meta
    d.item_coupon,
    d.item_affiliation

  from deduped d
  left join refund_items ri
    on ri.transaction_id = d.transaction_id
   and ri.item_key       = d.item_key
  left join {{ ref('dim_items') }} di
    on di.item_key       = d.item_key
)

select * from assembled