{{
  config(
    tags = ['adw']
    )
}}

-- models/staging/stg_res_comm.sql

WITH combined_showcase_plus_tables AS (
  {% set table_names = get_dynamic_table_names('%res_comm%') %}
  {% for table_name in table_names %}
    SELECT '{{ table_name }}' as source_table, * FROM `{{ target.project }}.adw_stage.{{ table_name }}`
    {% if not loop.last %}
    UNION ALL
    {% endif %}
  {% endfor %}
)

SELECT
  DISTINCT
  CONCAT(
  COALESCE(source_table, ''), '|',
  COALESCE(ID, ''), '|',
  COALESCE(Live_Date, '')
  ) AS AD_REPORT_KEY,
  Sales_REP_Field AS AD_CURRENT_REP_NAME,
  SAFE_CAST(
      REGEXP_REPLACE(Live_Date, r'^(\d{2})/(\d{2})/(\d{4})$', '\\3-\\2-\\1')
      AS DATE
    ) AS DATE,
  Office AS AD_ADVERTISER_NAME,
  ID AS AD_PRODUCT_ID,
  PRICE AS AD_REVENUE,
  RECORD_LOAD_DTS
FROM combined_showcase_plus_tables
WHERE Live_Date NOT IN ('Live Date', 'Issue / Run Date')
