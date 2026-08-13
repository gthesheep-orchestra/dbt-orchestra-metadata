{% macro json_extract_bool(column_name, path) %}
    {{ return(adapter.dispatch('json_extract_bool', 'orchestra_metadata')(column_name, path)) }}
{% endmacro %}

{% macro default__json_extract_bool(column_name, path) %}
    {% do exceptions.raise_compiler_error(
        'json_extract_bool is not implemented for adapter "' ~ adapter.type() ~ '". '
        ~ 'Add a ' ~ adapter.type() ~ '__json_extract_bool macro in the orchestra_metadata project.'
    ) %}
{% endmacro %}

{% macro bigquery__json_extract_bool(column_name, path) %}
    safe_cast(json_value({{ column_name }}, '$.{{ path }}') as bool)
{% endmacro %}

{% macro duckdb__json_extract_bool(column_name, path) %}
    cast(json_extract_string({{ column_name }}, '$.{{ path }}') as boolean)
{% endmacro %}
