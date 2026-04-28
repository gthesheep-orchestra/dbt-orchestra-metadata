{% macro most_common_value(expression) %}
    {{ return(adapter.dispatch('most_common_value', 'orchestra_metadata')(expression)) }}
{% endmacro %}

{% macro default__most_common_value(expression) %}
    {% do exceptions.raise_compiler_error(
        'most_common_value is not implemented for adapter "' ~ adapter.type() ~ '". '
        ~ 'Add a ' ~ adapter.type() ~ '__most_common_value macro in the orchestra_metadata project.'
    ) %}
{% endmacro %}

{% macro bigquery__most_common_value(expression) %}
    (approx_top_count({{ expression }}, 1)[offset(0)]).value
{% endmacro %}

{% macro duckdb__most_common_value(expression) %}
    mode({{ expression }})
{% endmacro %}
