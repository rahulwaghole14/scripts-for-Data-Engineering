{% macro get_dynamic_table_names(pattern) %}
    {% set query %}
    SELECT table_name
    FROM `{{ target.project}}.adw_stage.INFORMATION_SCHEMA.TABLES`
    WHERE table_name LIKE '{{ pattern }}'
  {% endset %}

    {% set results = run_query(query) %}
    {% if execute %}
    {% set table_names = results.columns[0].values() %}
  {% else %}
    {% set table_names = [] %}
  {% endif %}

    {{ return(table_names) }}
{% endmacro %}
