{% macro safe_cast_timestamp(column_name) %}
    {{ return(adapter.dispatch('safe_cast_timestamp', 'orchestra_metadata')(column_name)) }}
{% endmacro %}

{% macro default__safe_cast_timestamp(column_name) %}
    {% do exceptions.raise_compiler_error(
        'safe_cast_timestamp is not implemented for adapter "' ~ adapter.type() ~ '". '
        ~ 'Add a ' ~ adapter.type() ~ '__safe_cast_timestamp macro in the orchestra_metadata project.'
    ) %}
{% endmacro %}

{% macro bigquery__safe_cast_timestamp(column_name) %}
    safe_cast({{ column_name }} as timestamp)
{% endmacro %}

{% macro duckdb__safe_cast_timestamp(column_name) %}
    try_cast({{ column_name }} as timestamp)
{% endmacro %}
