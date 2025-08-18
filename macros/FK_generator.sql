{% macro make_event_key( ) %}
  {{ dbt_utils.generate_surrogate_key([
    'user_pseudo_id',
    'event_name',
    'cast(event_timestamp as string)',        
    'cast(event_bundle_sequence_id as string)',  
    'cast(stream_id as string)',
    "coalesce( (select p.value.int_value from unnest(event_params) p where p.key='ga_session_id'), -1 )"
  ]) }}
{% endmacro %}



{% macro make_item_key(item_idx, event_id, event_timestamp_utc) %}
  {{ dbt_utils.generate_surrogate_key([
    item_idx,
    event_id,
    event_timestamp_utc,
  ]) }}
{% endmacro %}


{% macro make_device_key(device_category, os, os_version, browser, browser_version) %}
  {{ dbt_utils.generate_surrogate_key([
    device_category,
    os,
    os_version,
    browser,
    browser_version,
  ]) }}
{% endmacro %}

{% macro make_geo_key(geo_country, geo_region, geo_city) %}
  {{ dbt_utils.generate_surrogate_key([
    geo_country,
    geo_region,
    geo_city,
  ]) }}
{% endmacro %}

{% macro make_traffic_key(traffic_source, traffic_medium, traffic_campaign, traffic_content, traffic_term) %}
  {{ dbt_utils.generate_surrogate_key([
    traffic_source,
    traffic_medium,
    traffic_campaign,
    traffic_content,
    traffic_term,
  ]) }}
{% endmacro %}


{% macro make_page_key(page_location) %}
  {{ dbt_utils.generate_surrogate_key([
    page_location,
  ]) }}
{% endmacro %}


{% macro make_date_key(event_date_dt) %}
  {{ dbt_utils.generate_surrogate_key([
    event_date_dt,
  ]) }}
{% endmacro %}

{% macro make_user_key(user_pseudo_id) %}
  {{ dbt_utils.generate_surrogate_key([
    user_pseudo_id,
  ]) }}
{% endmacro %}

{% macro make_session_key(user_pseudo_id, ga_session_id) %}
  {{ dbt_utils.generate_surrogate_key([
    user_pseudo_id,
    ga_session_id,
  ]) }}
{% endmacro %}