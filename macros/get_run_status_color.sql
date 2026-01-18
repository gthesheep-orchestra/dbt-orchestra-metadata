{% macro get_run_status_color(status_column) %}
{#
    Returns a color code for run statuses useful for visualization.
    Can be used in BI tools that support conditional formatting.
#}
    case {{ status_column }}
        when 'SUCCEEDED' then 'green'
        when 'FAILED' then 'red'
        when 'WARNING' then 'yellow'
        when 'RUNNING' then 'blue'
        when 'CREATED' then 'gray'
        when 'QUEUED' then 'gray'
        when 'CANCELLED' then 'orange'
        when 'CANCELLING' then 'orange'
        when 'SKIPPED' then 'gray'
        else 'gray'
    end
{% endmacro %}
