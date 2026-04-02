-- Generate MD5 hash key from multiple columns
{% macro hash_key(columns) %}
    MD5(CONCAT_WS('|', {% for col in columns %}CAST(COALESCE(CAST({{ col }} AS STRING), '') AS STRING){% if not loop.last %}, {% endif %}{% endfor %}))
{% endmacro %}
