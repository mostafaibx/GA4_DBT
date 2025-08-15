{% macro url_host(url_expr) %}
  regexp_extract({{ url_expr }}, r'^https?://([^/]+)')
{% endmacro %}

{% macro url_path_normalized(url_expr) %}
  -- Extract path and remove trailing slash (except root). Default to '/'
  case
    when regexp_extract({{ url_expr }}, r'^https?://[^/]+(/[^?#]*)') is null then '/'
    else regexp_replace(
           regexp_extract({{ url_expr }}, r'^https?://[^/]+(/[^?#]*)'),
           r'/$',
           ''
         )
  end
{% endmacro %}

{% macro url_query(url_expr) %}
  -- Extract query string without leading '?', stopping at '#'
  regexp_extract({{ url_expr }}, r'\?([^#]*)')
{% endmacro %}

{% macro url_query_strip_tracking(query_expr) %}
  -- Remove common tracking params (utm_*, gclid, fbclid, msclkid)
  (select array_to_string(
     array(
       select kv from unnest(split(coalesce({{ query_expr }}, ''), '&')) as kv
       where lower(split(kv,'=')[offset(0)]) not like 'utm_%'
         and lower(split(kv,'=')[offset(0)]) not in ('gclid','fbclid','msclkid')
     ), '&')
  )
{% endmacro %}

{% macro page_key_expr(url_expr) %}
  -- Stable page key from host + normalized path + cleaned query
  to_hex(md5(
    concat(
      lower(coalesce({{ url_host(url_expr) }}, '')),
      '|', coalesce({{ url_path_normalized(url_expr) }}, '/'),
      '|', coalesce({{ url_query_strip_tracking(url_query(url_expr)) }}, '')
    )
  ))
{% endmacro %}