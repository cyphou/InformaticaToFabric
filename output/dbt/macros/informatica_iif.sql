-- Convert Informatica IIF to SQL CASE WHEN
{% macro iif(condition, true_value, false_value) %}
    CASE WHEN {{ condition }} THEN {{ true_value }} ELSE {{ false_value }} END
{% endmacro %}
