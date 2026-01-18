{% macro json_array_length(column_name) %}
    {{ return(adapter.dispatch('json_array_length', 'orchestra_metadata')(column_name)) }}
{% endmacro %}

{% macro default__json_array_length(column_name) %}
    {# Default implementation using JSON_ARRAY_LENGTH - works for Snowflake, BigQuery #}
    json_array_length({{ column_name }})
{% endmacro %}

{% macro snowflake__json_array_length(column_name) %}
    array_size(parse_json({{ column_name }}))
{% endmacro %}

{% macro bigquery__json_array_length(column_name) %}
    array_length(json_extract_array({{ column_name }}))
{% endmacro %}

{% macro postgres__json_array_length(column_name) %}
    jsonb_array_length({{ column_name }}::jsonb)
{% endmacro %}

{% macro redshift__json_array_length(column_name) %}
    json_array_length({{ column_name }})
{% endmacro %}

{% macro duckdb__json_array_length(column_name) %}
    json_array_length({{ column_name }})
{% endmacro %}

{% macro databricks__json_array_length(column_name) %}
    size(from_json({{ column_name }}, 'array<string>'))
{% endmacro %}
