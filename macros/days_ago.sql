{% macro days_ago(n) %}
    {{ return(adapter.dispatch('days_ago', 'orchestra_metadata')(n)) }}
{% endmacro %}

{% macro default__days_ago(n) %}
    current_timestamp - interval '{{ n }} days'
{% endmacro %}

{% macro bigquery__days_ago(n) %}
    timestamp_sub(current_timestamp(), interval {{ n }} day)
{% endmacro %}

{% macro snowflake__days_ago(n) %}
    dateadd(day, -{{ n }}, current_timestamp())
{% endmacro %}
