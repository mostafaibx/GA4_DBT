{{
  config(
    materialized = 'incremental',
    unique_key   = 'event_id',
  )
}}
    -- on_schema_change = 'sync_all_columns',
    -- partition_by = {"field": "event_date_dt", "data_type": "date"},
    -- cluster_by   = ["event_name", "user_pseudo_id", "ga_session_id"],
    -- tags         = ['staging','ga4'], 

with source_data as (
  select
    *,
    _table_suffix as table_suffix
  from {{ source('ga4_export', 'events_*') }}
  where parse_date('%Y%m%d', _table_suffix) >= cast("2021-01-26" as date)
),
   -- {% if is_incremental() %}
     -- and parse_date('%Y%m%d', _table_suffix) >= date_sub(current_date(), interval {{ var("ga4_incremental_lookback_days", 7) }} day)
    -- {% endif %} 

renamed as (
  select
    -- Stable event key (macro should hash a deterministic set of natural keys)
    {{ make_event_key() }}                                                    as event_id,

    -- Dates & timestamps
    parse_date('%Y%m%d', event_date)                                          as event_date_dt,
    timestamp_micros(event_timestamp)                                         as event_timestamp_utc,
    timestamp_micros(event_previous_timestamp)                                as event_previous_timestamp_utc,

    -- Offset is in microseconds; keep raw offset and compute server-adjusted ts
    cast(event_server_timestamp_offset as int64)                              as event_server_timestamp_offset_us,
    timestamp_micros(event_timestamp + event_server_timestamp_offset)         as event_server_timestamp_utc,

    timestamp_micros(user_first_touch_timestamp)                              as user_first_touch_timestamp_utc,

    -- Event fields
    event_name,
    event_value_in_usd,
    event_bundle_sequence_id,

    -- User fields
    user_id,
    user_pseudo_id,
    privacy_info,
    user_ltv,

    -- Keep raw arrays/structs for downstream reuse
    user_properties,
    event_params,

    -- Common, high-value params (extracted safely)
    -- Session params
    (
      select cast(ep.value.int_value as int64)
      from unnest(event_params) ep
      where ep.key = 'ga_session_id'
      limit 1
    )                                                                         as ga_session_id,
    (
      select cast(ep.value.int_value as int64)
      from unnest(event_params) ep
      where ep.key = 'ga_session_number'
      limit 1
    )                                                                         as ga_session_number,

    -- Commerce params frequently needed in marts
    (
      select cast(ep.value.string_value as string)
      from unnest(event_params) ep
      where ep.key = 'transaction_id'
      limit 1
    )                                                                         as param_transaction_id,
    (
      select cast(coalesce(ep.value.string_value,
                           cast(ep.value.int_value as string),
                           cast(ep.value.double_value as string)) as string)
      from unnest(event_params) ep
      where ep.key = 'currency'
      limit 1
    )                                                                         as param_currency,

    -- Device
    device.category                           as device_category,
    device.mobile_brand_name                  as device_mobile_brand_name,
    device.mobile_model_name                  as device_mobile_model_name,
    device.mobile_marketing_name              as device_mobile_marketing_name,
    device.mobile_os_hardware_model           as device_mobile_os_hardware_model,
    device.operating_system                   as device_operating_system,
    device.operating_system_version           as device_operating_system_version,
    device.vendor_id                          as device_vendor_id,
    device.advertising_id                     as device_advertising_id,
    device.language                           as device_language,
    device.is_limited_ad_tracking             as device_is_limited_ad_tracking,
    device.time_zone_offset_seconds           as device_time_zone_offset_seconds,
    device.web_info.browser                   as device_web_browser,
    device.web_info.browser_version           as device_web_browser_version,

    -- Event dimensions (when present in export)
    event_dimensions.hostname                 as event_dimensions_hostname,

    -- Geography
    geo.continent                             as geo_continent,
    geo.sub_continent                         as geo_sub_continent,
    geo.country                               as geo_country,
    geo.region                                as geo_region,
    geo.metro                                 as geo_metro,
    geo.city                                  as geo_city,

    -- App info
    app_info.id                               as app_id,
    app_info.version                          as app_version,
    app_info.install_store                    as app_install_store,
    app_info.firebase_app_id                  as app_firebase_app_id,
    app_info.install_source                   as app_install_source,

    -- Traffic source
    traffic_source.name                       as traffic_source_name,
    traffic_source.medium                     as traffic_source_medium,
    traffic_source.source                     as traffic_source_source,
    {{ ga4_param_str('event_params', 'campaign') }} as traffic_campaign,
    {{ ga4_param_str('event_params', 'content') }} as traffic_content,
    {{ ga4_param_str('event_params', 'term') }} as traffic_term,

    -- Ecommerce struct (kept intact + useful top-levels)
    ecommerce.total_item_quantity             as ecommerce_total_item_quantity,
    ecommerce.purchase_revenue_in_usd         as ecommerce_purchase_revenue_in_usd,
    ecommerce.purchase_revenue                as ecommerce_purchase_revenue,
    ecommerce.refund_value_in_usd             as ecommerce_refund_value_in_usd,
    ecommerce.refund_value                    as ecommerce_refund_value,
    ecommerce.shipping_value_in_usd           as ecommerce_shipping_value_in_usd,
    ecommerce.shipping_value                  as ecommerce_shipping_value,
    ecommerce.tax_value_in_usd                as ecommerce_tax_value_in_usd,
    ecommerce.tax_value                       as ecommerce_tax_value,
    ecommerce.unique_items                    as ecommerce_unique_items,
    ecommerce.transaction_id                  as ecommerce_transaction_id,

    -- Collection information
    stream_id,
    platform,
    items,

    -- Metadata
    current_timestamp()                       as ingestion_timestamp,
    table_suffix
  from source_data
)

select * from renamed
