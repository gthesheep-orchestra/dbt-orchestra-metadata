{% macro percentile_cont(expression, fraction) %}
    {{ return(adapter.dispatch('percentile_cont', 'orchestra_metadata')(expression, fraction)) }}
{% endmacro %}

{% macro default__percentile_cont(expression, fraction) %}
    percentile_cont({{ fraction }}) within group (order by {{ expression }})
{% endmacro %}

{% macro bigquery__percentile_cont(expression, fraction) %}
    {# approx_quantiles is an aggregate function in BigQuery; percentile_cont is analytic-only #}
    approx_quantiles({{ expression }}, 100)[offset({{ (fraction * 100) | round | int }})]
{% endmacro %}

{% macro duckdb__percentile_cont(expression, fraction) %}
    quantile_cont({{ expression }}, {{ fraction }})
{% endmacro %}
