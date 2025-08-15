-- macros/fk_keys.sql
-- Purpose: one-stop macros to compute surrogate Foreign Keys (FKs)
--          for all dimensions used in your GA4 star schema.
--
-- These macros mirror the key recipes used in the dims we've built, so
-- facts can generate matching FKs consistently and cheaply.
--
-- Usage examples (inside a fact model):
--   select
--     {{ fk_user_key('e.user_pseudo_id') }}                           as user_key,
--     {{ fk_session_key('e.user_pseudo_id','e.ga_session_id') }}      as session_key,
--     {{ fk_traffic_session_key('e.user_pseudo_id','e.ga_session_id') }} as traffic_session_key,
--     {{ fk_page_key("{{ ga4_param_str('e.event_params','page_location') }}") }} as page_key,
--     {{ fk_item_key('it.product_item_id') }}                         as item_key,
--     {{ fk_geo_key('e.geo_continent','e.geo_sub_continent','e.geo_country','e.geo_region','e.geo_metro','e.geo_city') }} as geo_key_event,
--     {{ fk_device_key('e.device_category','e.device_operating_system','e.device_operating_system_version','e.device_web_browser','e.device_web_browser_version','e.device_mobile_brand_name','e.device_mobile_model_name','e.device_mobile_marketing_name','e.device_mobile_os_hardware_model','e.device_language','e.device_is_limited_ad_tracking') }} as device_key_event,
--     {{ fk_order_key('o.transaction_id') }}                          as order_key,
--     {{ fk_order_item_key('oi.transaction_id','oi.item_index') }}    as order_item_key
--   from ...
--
-- Note: pass column *expressions* (as strings) to each macro.

{% macro fk_user_key(user_pseudo_id_expr) %}
  {{ dbt_utils.generate_surrogate_key([ user_pseudo_id_expr ]) }}
{% endmacro %}

{% macro fk_session_key(user_pseudo_id_expr, ga_session_id_expr) %}
  {{ dbt_utils.generate_surrogate_key([ user_pseudo_id_expr, 'cast(' ~ ga_session_id_expr ~ ' as string)' ]) }}
{% endmacro %}

{% macro fk_traffic_session_key(user_pseudo_id_expr, ga_session_id_expr) %}
  -- Alias of fk_session_key; kept separate for clarity
  {{ fk_session_key(user_pseudo_id_expr, ga_session_id_expr) }}
{% endmacro %}

{# ---------------- GEO ---------------- #}
{% macro fk_geo_key(continent_expr, sub_continent_expr, country_expr, region_expr, metro_expr, city_expr) %}
  to_hex(md5(concat_ws('|',
    'ga4',
    coalesce({{ continent_expr }}, ''),
    coalesce({{ sub_continent_expr }}, ''),
    coalesce({{ country_expr }}, ''),
    coalesce({{ region_expr }}, ''),
    coalesce({{ metro_expr }}, ''),
    coalesce({{ city_expr }}, '')
  )))
{% endmacro %}

-- Convenience wrapper if you have an events alias with GA4 geo columns
{% macro fk_geo_from_event(alias) %}
  {{ fk_geo_key(alias ~ '.geo_continent', alias ~ '.geo_sub_continent', alias ~ '.geo_country', alias ~ '.geo_region', alias ~ '.geo_metro', alias ~ '.geo_city') }}
{% endmacro %}

{# --------------- DEVICE -------------- #}
{% macro fk_device_key(category_expr, os_expr, os_version_expr, browser_expr, browser_version_expr, brand_expr, model_expr, marketing_name_expr, hardware_model_expr, language_expr, limit_ad_tracking_expr) %}
  to_hex(md5(concat_ws('|',
    'ga4',
    lower(coalesce({{ category_expr }},'')),
    lower(coalesce({{ os_expr }},'')),
    lower(coalesce({{ os_version_expr }},'')),
    lower(coalesce({{ browser_expr }},'')),
    lower(coalesce({{ browser_version_expr }},'')),
    lower(coalesce({{ brand_expr }},'')),
    lower(coalesce({{ model_expr }},'')),
    lower(coalesce({{ marketing_name_expr }},'')),
    lower(coalesce({{ hardware_model_expr }},'')),
    coalesce({{ language_expr }},''),
    cast(coalesce({{ limit_ad_tracking_expr }}, false) as string)
  )))
{% endmacro %}

-- Convenience wrapper if you have an events alias with GA4 device columns
{% macro fk_device_from_event(alias) %}
  {{ fk_device_key(
      alias ~ '.device_category',
      alias ~ '.device_operating_system',
      alias ~ '.device_operating_system_version',
      alias ~ '.device_web_browser',
      alias ~ '.device_web_browser_version',
      alias ~ '.device_mobile_brand_name',
      alias ~ '.device_mobile_model_name',
      alias ~ '.device_mobile_marketing_name',
      alias ~ '.device_mobile_os_hardware_model',
      alias ~ '.device_language',
      alias ~ '.device_is_limited_ad_tracking'
  ) }}
{% endmacro %}

{# --------------- PAGE ---------------- #}
-- If you already have macros/url_normalize.sql with page_key_expr(url_expr),
-- reuse it here. This is just a friendly alias for FK generation.
{% macro fk_page_key(url_expr) %}
  {{ page_key_expr(url_expr) }}
{% endmacro %}

-- Convenience wrapper when page_location lives in event_params
{% macro fk_page_from_params(params_expr) %}
  {{ fk_page_key(ga4_param_str(params_expr, 'page_location')) }}
{% endmacro %}

{# --------------- ITEM ---------------- #}
{% macro fk_item_key(product_item_id_expr) %}
  {{ dbt_utils.generate_surrogate_key([ "'ga4'", product_item_id_expr ]) }}
{% endmacro %}

{# --------------- ORDER / LINES ------- #}
{% macro fk_order_key(transaction_id_expr) %}
  {{ dbt_utils.generate_surrogate_key([ transaction_id_expr ]) }}
{% endmacro %}

{% macro fk_order_item_key(transaction_id_expr, item_index_expr) %}
  {{ dbt_utils.generate_surrogate_key([ transaction_id_expr, 'cast(' ~ item_index_expr ~ ' as string)' ]) }}
{% endmacro %}

{# --------------- TRAFFIC (USER FIRST) #}
-- If you add a user-first-touch traffic dim, this FK maps to user_pseudo_id.
{% macro fk_traffic_user_first_key(user_pseudo_id_expr) %}
  {{ dbt_utils.generate_surrogate_key([ user_pseudo_id_expr ]) }}
{% endmacro %}
