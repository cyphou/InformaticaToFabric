-- Generate surrogate key using dbt_utils
{% macro surrogate_key(columns) %}
    {{ dbt_utils.generate_surrogate_key(columns) }}
{% endmacro %}
