{% macro source_column_or_null(relation, column_name) %}
    {% if execute %}
        {% set source_columns = adapter.get_columns_in_relation(relation) %}
        {% set source_column_names = source_columns | map(attribute='name') | map('lower') | list %}

        {% if column_name | lower in source_column_names %}
            {{ column_name }}
        {% else %}
            null
        {% endif %}
    {% else %}
        null
    {% endif %}
{% endmacro %}
