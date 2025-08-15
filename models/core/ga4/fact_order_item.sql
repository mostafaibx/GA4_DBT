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
    ev.device_os_version,
    ev.browser,
    ev.browser_version,
    ev.geo_country,
    ev.geo_region,
    ev.geo_city,
    ev.geo_language,
    ev.traffic_source,
    ev.traffic_medium,
    ev.traffic_campaign,
    ev.traffic_content,
    ev.traffic_term
    ev.event_date_dt,
    ev.page_location
    ev.traffic_source,
    ev.traffic_medium,
    ev.traffic_campaign,
    ev.traffic_content,
    ev.traffic_term




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
             order by pi.order_ts desc, pi.event_id desc
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
    it.product_item_id,
    sum(coalesce(it.item_quantity, 0))             as refunded_item_quantity,
    sum(coalesce(it.item_revenue, 0))              as refunded_item_revenue  -- often negative in staging; we will subtract
  from {{ ref('stg_ga4_items') }} it
  join {{ ref('stg_ga4_events') }} ev
    on ev.event_id = it.event_id
  where ev.event_name = 'refund'
    and ev.param_transaction_id is not null
  {% if is_incremental() %}
    and ev.event_date_dt >= date_sub(current_date(), interval {{ lookback_days }} day)
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
    {{ make_user_key('pid.user_pseudo_id') }}                            as user_key,
    {{ make_session_key('pid.user_pseudo_id','pid.ga_session_id') }}     as session_key,
    {{ make_date_key('date(pid.order_ts)') }}                            as date_key,
    {{ make_device_key('pid.device_category','pid.device_os','pid.device_os_version','pid.browser','pid.browser_version') }} as device_key,
    {{ make_geo_key('pid.geo_country','pid.geo_region','pid.geo_city','pid.geo_language') }}                                   as geo_key,
    {{ make_traffic_key('pid.traffic_source','pid.traffic_medium','pid.traffic_campaign','pid.traffic_content','pid.traffic_term') }} as traffic_key,

    -- natural identifiers & timing
    pid.transaction_id,
    pid.user_pseudo_id,
    pid.ga_session_id,
    pid.currency,
    pid.order_ts,
    date(pid.order_ts)                                                   as order_date,

    -- product keys + canonical attributes from the dimension (if present)
    pid.item_key,
    coalesce(di.product_item_id, pid.product_item_id)                    as product_item_id,
    coalesce(di.item_name,       pid.item_name)                          as item_name,
    coalesce(di.item_brand,      pid.item_brand)                         as item_brand,
    coalesce(di.item_variant,    pid.item_variant)                       as item_variant,
    coalesce(di.item_category1,  pid.item_category1)                     as item_category1,
    coalesce(di.item_category2,  pid.item_category2)                     as item_category2,
    coalesce(di.item_category3,  pid.item_category3)                     as item_category3,
    coalesce(di.item_category4,  pid.item_category4)                     as item_category4,
    coalesce(di.item_category5,  pid.item_category5)                     as item_category5,

    -- item economics (purchase)
    pid.item_index,
    pid.item_price,
    pid.item_quantity,
    pid.item_revenue,

    -- refund economics (item level)
    coalesce(ri.refunded_item_quantity, 0)                               as refunded_item_quantity,
    coalesce(ri.refunded_item_revenue, 0)                                as refunded_item_revenue,

    -- net amounts (purchase minus refunds)
    greatest(pid.item_quantity - coalesce(ri.refunded_item_quantity, 0), 0) as net_item_quantity,
    (pid.item_revenue - coalesce(ri.refunded_item_revenue, 0))              as net_item_revenue,

    -- derived KPIs
    greatest(coalesce(pid.item_price, 0) * coalesce(pid.item_quantity, 0) - coalesce(pid.item_revenue, 0), 0) as item_discount_value,
    safe_divide(pid.item_revenue, nullif(pid.item_quantity, 0))          as purchase_unit_price,
    safe_divide((pid.item_revenue - coalesce(ri.refunded_item_revenue, 0)),
                nullif(greatest(pid.item_quantity - coalesce(ri.refunded_item_quantity, 0), 0), 0))          as effective_unit_price,

    -- optional meta
    pid.item_coupon,
    pid.item_affiliation

  from deduped pid
  left join refund_items ri
    on ri.transaction_id = pid.transaction_id
   and ri.item_key       = pid.item_key
  left join {{ ref('dim_items') }} di
    on di.item_key       = pid.item_key
)

select * from assembled