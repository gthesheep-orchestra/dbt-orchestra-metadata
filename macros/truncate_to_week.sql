{% macro truncate_to_week(column_name) %}
    {{ return(adapter.dispatch('truncate_to_week', 'orchestra_metadata')(column_name)) }}
{% endmacro %}

{% macro default__truncate_to_week(column_name) %}
    {# Most warehouses default date_trunc('week', ...) to a Monday start already #}
    date_trunc('week', {{ column_name }})
{% endmacro %}

{% macro bigquery__truncate_to_week(column_name) %}
    {# BigQuery's WEEK part defaults to a Sunday start, so pin it to Monday #}
    date_trunc({{ column_name }}, week(monday))
{% endmacro %}
