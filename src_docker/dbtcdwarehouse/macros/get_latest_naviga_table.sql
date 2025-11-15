{% macro get_latest_table() %}
    {%- call statement('latest_table', fetch_result=True) -%}
    SELECT right(table_name, 8)
    FROM `hexa-data-report-etl-prod.adw_stage.INFORMATION_SCHEMA.TABLES`
    WHERE table_name LIKE 'naviga__adrevenue_bq_%'
    ORDER BY table_name DESC
    LIMIT 1;
    {%- endcall -%}

    {% set result = load_result('latest_table') %}

    {% if result and result.table and result.table.columns[0].values()|length > 0 %}
    {{ return(result.table.columns[0].values()[0]) }}
  {% else %}
    {{ return('DEFAULT_TABLE') }}
  {% endif %}
{% endmacro %}
