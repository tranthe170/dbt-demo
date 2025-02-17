{% macro concat(fields) -%}
  {{ adapter.dispatch('concat')(fields) }}
{%- endmacro %}

{% macro bigquery__concat(fields) -%}
    {#-- concat() in SQL bigquery scales better with number of columns than using the '||' operator --#}
    concat({{ fields|join(', ') }})
{%- endmacro %}

{% macro mysql__concat(fields) -%}
    {#-- MySQL doesn't support the '||' operator as concatenation by default --#}
    concat({{ fields|join(', ') }})
{%- endmacro %}

{% macro sqlserver__concat(fields) -%}
    {#-- CONCAT() in SQL SERVER accepts from 2 to 254 arguments, we use batches for the main concat, to overcome the limit. --#}
    {% set concat_chunks = [] %}
    {% for chunk in fields|batch(253) -%}
        {% set _ = concat_chunks.append( "concat(" ~ chunk|join(', ') ~ ",'')" ) %}
    {% endfor %}

    concat({{ concat_chunks|join(', ') }}, '')
{%- endmacro %}

{% macro tidb__concat(fields) -%}
    concat({{ fields|join(', ') }})
{%- endmacro %}

{% macro duckdb__concat(fields) -%}
    concat({{ fields|join(', ') }})
{%- endmacro %}