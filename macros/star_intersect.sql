{% macro star_intersect(from, intersect, from_alias=False, intersect_alias=False, except=[]) -%}
    {%- do dbt_utils._is_relation(from, 'star_intersect') -%}
    {%- do dbt_utils._is_ephemeral(from, 'star_intersect') -%}
    {%- do dbt_utils._is_relation(intersect, 'star_intersect') -%}
    {%- do dbt_utils._is_ephemeral(intersect, 'star_intersect') -%}

    {#-- Prevent querying of db in parsing mode. This works because this macro does not create any new refs. #}
    {%- if not execute -%}
        {{ return('') }}
    {% endif %}

    {%- set include_cols = [] %}
    {%- set cols = adapter.get_columns_in_relation(from) -%}
    {%- set except = except | map("lower") | list %}
    {%- for col in cols -%}
        {%- if col.column|lower not in except -%}
            {% do include_cols.append(col.column) %}
        {%- endif %}
    {%- endfor %}

    {%- set include_intersect_cols = [] %}
    {%- set intersect_cols = adapter.get_columns_in_relation(intersect) -%}
    {%- for col in intersect_cols -%}
        {%- if col.column|lower not in except -%}
            {% do include_intersect_cols.append(col.column) %}
        {%- endif %}
    {%- endfor %}

    {%- for col in include_cols %}
        {%- if col in include_intersect_cols -%}
            {%- if intersect_alias %}{{ intersect_alias }}.{% else %}{%- endif -%}{{ adapter.quote(col)|trim }}
            {%- if not loop.last %},{{ '\n  ' }}{% endif %}
        {%- else %}
            {%- if from_alias %}{{ from_alias }}.{% else %}{{ from }}.{%- endif -%}{{ adapter.quote(col)|trim }} as {{ adapter.quote(col)|trim }}
            {%- if not loop.last %},{{ '\n  ' }}{% endif %}
        {%- endif %}
    {%- endfor -%}
{%- endmacro %}