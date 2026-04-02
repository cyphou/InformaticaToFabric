-- Clean and standardize string values
{% macro clean_string(column_name) %}
    TRIM(UPPER(REGEXP_REPLACE({{ column_name }}, r'\s+', ' ')))
{% endmacro %}
