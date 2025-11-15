This README provides documentation a Jinja macros used for database operations. These macros facilitate fetching specific information from the database tables, optimizing data retrieval processes.

## uuidv5 macro
https://github.com/Wuerike/BigQuery-GENERATE-UUID-V5/blob/main/GENERATE_UUID_V5.sql
## Macro.: get_latest_table_suffix

This macro is designed to fetch the latest `TABLE_SUFFIX_VALUE` from a specified table in a database.

### Description

The `get_latest_table_suffix` macro executes a SQL query to retrieve the most recent `TABLE_SUFFIX_VALUE` from the `hexa-data-report-etl-prod.rbhaskhar_dw_intermediate.sat_adobe_analytics_masthead` table. This table is expected to have a column named `TABLE_SUFFIX_VALUE`, which stores timestamped or versioned suffixes in a descending order. The macro fetches the topmost record, implying the latest entry.

### Structure

```jinja
{% macro get_latest_table_suffix() %}
  {%- call statement('latest_table', fetch_result=True) -%}
    SELECT TABLE_SUFFIX_VALUE
    FROM `x.sat_adobe_analytics_masthead`
    ORDER BY TABLE_SUFFIX_VALUE DESC
    LIMIT 1;
  {%- endcall -%}

  {% set result = load_result('latest_table') %}

  {% if result and result.table and result.table.rows|length > 0 %}
    {{ return(result.table.rows[0][0]) }}
  {% else %}
    {{ return('latest_suffix') }}
  {% endif %}
{% endmacro %}
