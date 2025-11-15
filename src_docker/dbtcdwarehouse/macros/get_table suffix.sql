{# This Macro is designed to return the latest suffix of the data loaded in the satallite table.
 If the table doesnt exist then it will return a default value #}

{% macro get_table_suffix(table_name) %}

  {% if table_name == 'TABLE DOES NOT EXIST' %}
    {{ return('2023100800') }}
  {% else %}
    {%- call statement('latest_suffix', fetch_result=True) -%}
      SELECT MAX(TABLE_SUFFIX_VALUE)
      FROM `{{ target.project}}.{{ target.dataset }}_dw_intermediate.{{ table_name }}`
    {%- endcall -%}

    {% set result = load_result('latest_suffix') %}

    {% if result and result.table and result.table.columns[0].values()|length > 0 %}
      {{ return(result.table.columns[0].values()[0]) }}
    {% else %}
      {{ return('2023100800') }}
    {% endif %}
  {% endif %}
{% endmacro %}
