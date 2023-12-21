{% macro incremental_clause(col_emitted_at, tablename)  -%}
  {{ adapter.dispatch('incremental_clause')(col_emitted_at, tablename) }}
{%- endmacro %}

{%- macro default__incremental_clause(col_emitted_at, tablename) -%}
{% if is_incremental() %}
and coalesce(
    cast({{ col_emitted_at }} as timestamp with time zone) > (select max(cast({{ col_emitted_at }} as timestamp with time zone)) from {{ tablename }}),
    true)
{% endif %}
{%- endmacro -%}

{%- macro snowflake__incremental_clause(col_emitted_at, tablename) -%}
{% if is_incremental() %}
    {% if get_max_normalized_cursor(col_emitted_at, tablename) %}
and cast({{ col_emitted_at }} as timestamp with time zone) >
    cast('{{ get_max_normalized_cursor(col_emitted_at, tablename) }}' as timestamp with time zone)
    {% endif %}
{% endif %}
{%- endmacro -%}

{%- macro bigquery__incremental_clause(col_emitted_at, tablename) -%}
{% if is_incremental() %}
    {% if get_max_normalized_cursor(col_emitted_at, tablename) %}
and cast({{ col_emitted_at }} as timestamp with time zone) >
    cast('{{ get_max_normalized_cursor(col_emitted_at, tablename) }}' as timestamp with time zone)
    {% endif %}
{% endif %}
{%- endmacro -%}

{%- macro sqlserver__incremental_clause(col_emitted_at, tablename) -%}
{% if is_incremental() %}
and ((select max(cast({{ col_emitted_at }} as timestamp with time zone)) from {{ tablename }}) is null
  or cast({{ col_emitted_at }} as timestamp with time zone) >
     (select max(cast({{ col_emitted_at }} as timestamp with time zone)) from {{ tablename }}))
and ((select max(cast({{ col_emitted_at }} as timestamp with time zone)) from {{ tablename }}) is null
  or cast({{ col_emitted_at }} as timestamp with time zone) >
     (select current_timestamp - interval '24' hour + max(cast({{ col_emitted_at }} as timestamp with time zone)) from {{ tablename }}))
{% endif %}
{%- endmacro -%}

{% macro get_max_normalized_cursor(col_emitted_at, tablename) %}
{% if execute and is_incremental() %}
 {% if env_var('INCREMENTAL_CURSOR', 'UNSET') == 'UNSET' %}
     {% set query %}
        select max(cast({{ col_emitted_at }} as timestamp with time zone)) from {{ tablename }}
     {% endset %}
     {% set max_cursor = run_query(query).columns[0][0] %}
     {% do return(max_cursor) %}
 {% else %}
    {% do return(env_var('INCREMENTAL_CURSOR')) %}
 {% endif %}
{% endif %}
{% endmacro %}
