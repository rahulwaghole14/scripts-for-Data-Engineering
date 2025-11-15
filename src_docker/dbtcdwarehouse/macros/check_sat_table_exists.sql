{# This macro is desiged to check of the satallite table exists.
If it exists, thne it will return the name of the table
else it will return 'TABLE DOES NOT EXIST' #}

{% macro check_sat_table_exists() %}

  {# Get the table details if avaliable using information schema #}

  {%- call statement('table_getter', fetch_result=True) -%}
    SELECT table_name
    FROM `{{ target.project}}.{{ target.dataset }}_dw_intermediate.INFORMATION_SCHEMA.TABLES`
    WHERE table_name = 'sat_adobe_analytics_masthead'  -- Corrected the WHERE clause
    LIMIT 1;
  {%- endcall -%}

  {# Load result of query #}

  {% set result = load_result('table_getter') %}

  {% if result and result.table and result.table.columns[0].values()|length > 0 %}
    {{ return(result.table.columns[0].values()[0]) }}
  {% else %}
    {{ return('TABLE DOES NOT EXIST') }}
  {% endif %}
{% endmacro %}
