{#
    GA4 Parameter Unnesting Macro
    =============================
    
    Purpose: Transforms GA4s key-value parameter arrays into individual columns
    
    How it works:
    1. Takes an array of parameters (event_params or user_properties)
    2. For each parameter, creates a CASE statement that extracts the value
    3. Uses ANY_VALUE() aggregation to collapse multiple rows into one row per event
       (ANY_VALUE is more efficient than MAX since we only have one value per key)
    
    Parameters:
    - column_name: The name of the unnested column alias (e.g., 'ep' for event_params)
    - key_value_list: List of tuples defining which parameters to extract
      Format: [(key_name, data_type, optional_alias), ...]
      Example: [('page_location', 'string'), ('ga_session_id', 'int')]
    - value_type: Default data type if not specified in key_value_list ('auto' tries all types)
    
    Example SQL output:
    any_value(case when ep.key = 'page_location' then ep.value.string_value end) as page_location
#}
{% macro unnest_key_value_params(column_name, key_value_list, value_type='auto') %}
  {%- for key_value in key_value_list -%}
    {%- set key = key_value[0] -%}
    {%- set value_field = key_value[1] if key_value|length > 1 else value_type -%}
    {%- set alias = key_value[2] if key_value|length > 2 else key -%}
    
    any_value(case when {{ column_name }}.key = '{{ key }}' 
        then 
            {%- if value_field == 'auto' %}
            coalesce(
                {{ column_name }}.value.string_value,
                cast({{ column_name }}.value.int_value as string),
                cast({{ column_name }}.value.double_value as string),
                cast({{ column_name }}.value.float_value as string)
            )
            {%- elif value_field == 'string' %}
            {{ column_name }}.value.string_value
            {%- elif value_field == 'int' %}
            {{ column_name }}.value.int_value
            {%- elif value_field == 'double' %}
            {{ column_name }}.value.double_value
            {%- elif value_field == 'float' %}
            {{ column_name }}.value.float_value
            {%- else %}
            {{ column_name }}.value.{{ value_field }}_value
            {%- endif %}
        end) as {{ alias }}
    {%- if not loop.last %},{% endif %}
  {%- endfor -%}
{% endmacro %}


{#
    Event Parameters Pivot Macro
    ============================
    
    Purpose: Provides a pre-configured list of common GA4 event parameters to pivot
    
    This macro wraps the generic unnest_key_value_params macro with GA4-specific
    event parameter configurations. It includes the most commonly used parameters
    across web, mobile, and enhanced ecommerce implementations.
    
    Parameters:
    - event_params_config: Optional custom list of parameters to extract
      If not provided, uses the comprehensive default list below
    
    Usage:
    {{ pivot_event_params_to_columns() }}  -- Uses defaults
    {{ pivot_event_params_to_columns([('custom_param', 'string')]) }}  -- Custom params
    
    Note: Parameters are prefixed (e.g., 'param_value') to avoid conflicts with 
    standard event fields when the key name might clash
#}
{% macro pivot_event_params_to_columns(event_params_config=none) %}
  {%- set default_params = [
    ('page_location', 'string'),
    ('page_referrer', 'string'),
    ('page_title', 'string'),
    
    ('ga_session_id', 'int'),
    ('ga_session_number', 'int'),
    ('engaged_session_event', 'int'),
    ('engagement_time_msec', 'int'),
    ('session_engaged', 'string'),
    
    ('percent_scrolled', 'int'),
    
    ('value', 'double', 'param_value'),
    ('currency', 'string', 'param_currency'),
    ('transaction_id', 'string', 'param_transaction_id'),
    ('affiliation', 'string', 'param_affiliation'),
    ('coupon', 'string', 'param_coupon'),
    ('payment_type', 'string', 'param_payment_type'),
    
    ('content_type', 'string'),
    ('content_id', 'string'),
    ('content_group', 'string'),
    
    ('source', 'string', 'param_source'),
    ('medium', 'string', 'param_medium'),
    ('campaign', 'string', 'param_campaign'),
    ('term', 'string', 'param_term'),
    ('content', 'string', 'param_content'),
    
    ('gclid', 'string'),
    ('dclid', 'string'),
    ('srsltid', 'string'),
    
    ('search_term', 'string'),
    ('search_results', 'int'),
    
    ('video_current_time', 'int'),
    ('video_duration', 'int'),
    ('video_percent', 'int'),
    ('video_provider', 'string'),
    ('video_title', 'string'),
    ('video_url', 'string'),
    
    ('visible', 'string'),
    ('file_extension', 'string'),
    ('file_name', 'string'),
    ('link_classes', 'string'),
    ('link_domain', 'string'),
    ('link_id', 'string'),
    ('link_text', 'string'),
    ('link_url', 'string'),
    ('outbound', 'string'),
    
    ('debug_mode', 'int'),
    ('debug_target', 'string'),
    ('method', 'string'),
    ('entrances', 'int'),
    ('ignore_referrer', 'string'),
    
    ('campaign_content', 'string'),
    ('campaign_id', 'string'),
    ('campaign_source', 'string'),
    ('campaign_medium', 'string'),
    ('campaign_name', 'string'),
    ('campaign_term', 'string')
  ] -%}
  
  {%- set params_to_use = event_params_config if event_params_config else default_params -%}
  
  {{ unnest_key_value_params('ep', params_to_use) }}
{% endmacro %}


{#
    User Properties Pivot Macro
    ===========================
    
    Purpose: Provides a pre-configured list of common user properties to pivot
    
    Similar to event parameters, this macro handles the transformation of user
    properties from key-value arrays to individual columns. The default list
    includes common user attributes used for segmentation and analysis.
    
    Parameters:
    - user_properties_config: Optional custom list of properties to extract
    
    Usage:
    {{ pivot_user_properties_to_columns() }}  -- Uses defaults
    {{ pivot_user_properties_to_columns([('vip_status', 'string')]) }}  -- Custom properties
#}
{% macro pivot_user_properties_to_columns(user_properties_config=none) %}
  {%- set default_props = [
    ('user_id', 'string', 'user_id'),
    ('user_type', 'string'),
    ('user_status', 'string'),
    ('subscription_status', 'string'),
    ('subscription_type', 'string'),
    ('loyalty_status', 'string'),
    
    ('customer_lifetime_value', 'double'),
    ('first_purchase_date', 'string'),
    ('last_purchase_date', 'string'),
    ('total_purchases', 'int'),
    
    ('user_segment', 'string'),
    ('user_cohort', 'string'),
    
    ('preferred_language', 'string'),
    ('account_creation_method', 'string'),
    ('email_subscribe_status', 'string'),
    ('push_subscribe_status', 'string')
  ] -%}
  
  {%- set props_to_use = user_properties_config if user_properties_config else default_props -%}
  
  {{ unnest_key_value_params('up', props_to_use) }}
{% endmacro %}


{#
    Event Parameters CTE Creator
    ============================
    
    Purpose: Creates a Common Table Expression (CTE) that pivots event parameters
    
    This helper macro generates a complete CTE that:
    1. Unnests the event_params array
    2. Pivots key-value pairs into columns
    3. Groups by event_id to maintain one row per event
    
    Parameters:
    - cte_name: Name of the CTE (default: 'event_params_pivoted')
    - source_cte: Name of the CTE containing events data (default: 'events')
    
    Example usage in a model:
    with events as (
        select * from {{ ref('stg_ga4__events') }}
    ),
    {{ create_event_params_cte() }}
    select * from event_params_pivoted
#}
{% macro create_event_params_cte(cte_name='event_params_pivoted', source_cte='events') %}
{{ cte_name }} as (
    select
        event_id,
        {{ pivot_event_params_to_columns() }}
    from {{ source_cte }},
    unnest(event_params) as ep
    group by 1
)
{% endmacro %}


{#
    User Properties CTE Creator
    ===========================
    
    Purpose: Creates a Common Table Expression (CTE) that pivots user properties
    
    Similar to create_event_params_cte, but for user properties.
    Maintains the same pattern for consistency.
    
    Parameters:
    - cte_name: Name of the CTE (default: 'user_properties_pivoted')
    - source_cte: Name of the CTE containing events data (default: 'events')
    
    Note: Both CTEs can be used together to enrich events with both
    event parameters and user properties in a single query
#}
{% macro create_user_properties_cte(cte_name='user_properties_pivoted', source_cte='events') %}
{{ cte_name }} as (
    select
        event_id,
        {{ pivot_user_properties_to_columns() }}
    from {{ source_cte }},
    unnest(user_properties) as up
    group by 1
)
{% endmacro %}