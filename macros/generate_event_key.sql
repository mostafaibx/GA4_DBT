

{% macro make_event_key() %}
  {{ dbt_utils.generate_surrogate_key([
    'user_pseudo_id',
    'event_name',
    'cast(event_timestamp as string)',        
    'cast(event_bundle_sequence_id as string)',  
    'cast(stream_id as string)',
    "coalesce( (select p.value.int_value from unnest(event_params) p where p.key='ga_session_id'), -1 )"
  ]) }}
{% endmacro %}



{% macro make_item_key() %}
  {{ dbt_utils.generate_surrogate_key([
    'item_idx',
    'event_id',
    'cast(event_timestamp_utc as string)',
  ]) }}
{% endmacro %}