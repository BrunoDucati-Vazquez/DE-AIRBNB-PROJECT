{% macro tag(column_name) %}
    CASE
        WHEN {{ column_name }} < 100 THEN 'LOW'
        WHEN {{ column_name }} < 200 THEN 'MEDIUM'
        ELSE 'HIGH'
    END
{% endmacro %}