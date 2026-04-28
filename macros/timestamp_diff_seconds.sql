{% macro timestamp_diff_seconds(end_col, start_col) %}
    {{ return(adapter.dispatch('timestamp_diff_seconds', 'orchestra_metadata')(end_col, start_col)) }}
{% endmacro %}

{% macro default__timestamp_diff_seconds(end_col, start_col) %}
    {% do exceptions.raise_compiler_error(
        'timestamp_diff_seconds is not implemented for adapter "' ~ adapter.type() ~ '". '
        ~ 'Add a ' ~ adapter.type() ~ '__timestamp_diff_seconds macro in the orchestra_metadata project.'
    ) %}
{% endmacro %}

{% macro bigquery__timestamp_diff_seconds(end_col, start_col) %}
    timestamp_diff({{ end_col }}, {{ start_col }}, second)
{% endmacro %}

{% macro duckdb__timestamp_diff_seconds(end_col, start_col) %}
    date_diff('second', {{ start_col }}, {{ end_col }})
{% endmacro %}
