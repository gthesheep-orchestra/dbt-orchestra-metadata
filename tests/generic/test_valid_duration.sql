{% test valid_duration(model, column_name) %}
{#
    Test that duration values are non-negative.
    Negative durations indicate a data quality issue.
#}

select
    {{ column_name }}
from {{ model }}
where {{ column_name }} < 0

{% endtest %}
