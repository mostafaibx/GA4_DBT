{% macro ga4_param_str(params_expr, key) %}
  (
    select coalesce(p.value.string_value,
                    cast(p.value.int_value as string),
                    cast(p.value.double_value as string))
    from unnest({{ params_expr }}) as p
    where p.key = '{{ key }}'
    limit 1
  )
{% endmacro %}

{% macro ga4_param_int(params_expr, key) %}
  (
    select cast(coalesce(p.value.int_value,
                         cast(p.value.double_value as int64)) as int64)
    from unnest({{ params_expr }}) as p
    where p.key = '{{ key }}'
    limit 1
  )
{% endmacro %}