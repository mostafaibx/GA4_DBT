{{
    config(
        materialized='view',
    )
}}




with events as (
    select 
        event_id,
        user_pseudo_id,
        event_name,
        event_date_dt,        
        event_timestamp_utc,
        event_previous_timestamp_utc,
        event_server_timestamp_utc,
        user_first_touch_timestamp_utc, 
        event_params,
        items,

    (
      select cast(ep.value.string_value as string)
      from unnest(event_params) ep
      where ep.key = 'transaction_id'
      limit 1
    )              as param_transaction_id,

    (
      select cast(ep.value.string_value as string)
      from unnest(event_params) ep
      where ep.key = 'currency'
      limit 1
    )              as param_currency,

    from {{ ref('stg_ga4_events') }}
    where array_length(items) > 0
),


unnested_items as (
    select
        *,
        it as item,
        item_idx as item_index
    from events e
    cross join unnest(e.items) as it with offset as item_idx
    where it.item_id is not null
),




renamed as (
    select
        event_id,
        {{ make_item_key() }} as item_key,
        user_pseudo_id,
        event_name,

        event_date_dt,
        -- Timestamps already in UTC from source
        event_timestamp_utc,
        event_previous_timestamp_utc,
        event_server_timestamp_utc,
        user_first_touch_timestamp_utc,

        -- items fields
        item_index,
        item.item_id as product_item_id,
        item.item_name as item_name,
        item.item_variant as item_variant,
        item.item_brand as item_brand,
        item.item_category as item_category1,
        item.item_category2 as item_category2,
        item.item_category3 as item_category3,
        item.item_category4 as item_category4,
        item.item_category5 as item_category5,
        item.price_in_usd as item_price_in_usd,
        cast(item.price as float64) as item_price,
        cast(item.quantity as int64) as item_quantity,
        cast(item.item_revenue as float64) as item_revenue,
        cast(item.item_revenue_in_usd as float64) as item_revenue_in_usd,
        cast(item.item_refund as float64) as item_refund,
        cast(item.item_refund_in_usd as float64) as item_refund_in_usd,
        item.coupon as item_coupon,
        item.affiliation as item_affiliation,
        cast(item.location_id as int64) as item_location_id,
        item.item_list_id as item_list_id,
        item.item_list_name as item_list_name,
        item.item_list_index as item_list_index,
        item.promotion_id as item_promotion_id,
        item.promotion_name as item_promotion_name,
        item.creative_name as item_creative_name,
        item.creative_slot as item_creative_slot,
        param_transaction_id,
        param_currency,

    from unnested_items
)


select * from renamed