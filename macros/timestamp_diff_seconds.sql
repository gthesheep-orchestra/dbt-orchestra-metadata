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
    {#- dlt sometimes infers a timestamp-like source column as STRING (e.g. when
       early-loaded rows were null or malformed), so cast both sides defensively
       rather than assuming the source column already typed as TIMESTAMP. -#}
    timestamp_diff({{ safe_cast_timestamp(end_col) }}, {{ safe_cast_timestamp(start_col) }}, second)
{% endmacro %}

{% macro duckdb__timestamp_diff_seconds(end_col, start_col) %}
    date_diff('second', {{ safe_cast_timestamp(start_col) }}, {{ safe_cast_timestamp(end_col) }})
{% endmacro %}
