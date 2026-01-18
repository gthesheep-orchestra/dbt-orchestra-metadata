{% test timestamp_order(model, column_name, before_column) %}
{#
    Test that one timestamp comes before another.
    For example, started_at should be before completed_at.
#}

select
    {{ column_name }},
    {{ before_column }}
from {{ model }}
where {{ column_name }} is not null
  and {{ before_column }} is not null
  and {{ column_name }} > {{ before_column }}

{% endtest %}
