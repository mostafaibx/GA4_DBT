{#
    GA4 Timestamp Conversion Macro
    =============================
    
    Purpose: Convert GA4 microsecond timestamps to TIMESTAMP type with UTC timezone
    
    Usage: {{ convert_ga4_timestamp('event_timestamp') }}
    Output: timestamp_micros(event_timestamp) as event_timestamp_utc
#}

{% macro convert_ga4_timestamp(column_name, alias_suffix='_utc') %}
    timestamp_micros({{ column_name }}) as {{ column_name }}{{ alias_suffix }}
{% endmacro %}

{#
    Convert multiple GA4 timestamps at once
    
    Usage: {{ convert_ga4_timestamps(['event_timestamp', 'user_first_touch_timestamp']) }}
#}

{% macro convert_ga4_timestamps(timestamp_columns, alias_suffix='_utc') %}
    {%- for column in timestamp_columns -%}
        {{ convert_ga4_timestamp(column, alias_suffix) }}
        {%- if not loop.last %},{% endif %}
    {%- endfor -%}
{% endmacro %}