{{ config(
    tags=['smi'],
    materialized='table',
    on_schema_change='sync_all_columns'
) }}

WITH hexavstma AS (
    SELECT
    ProductCategory,
    MasterMediaType,
    MediaType,
    MediaSubType,
    State,
    YearMonth,
    SAFE.PARSE_DATETIME('%Y%m%d', CONCAT(CAST(YearMonth AS STRING), '01')) AS YearMonth_DATETIME,
    SAFE_CAST(Total_Market_Spend AS FLOAT64) AS Total_Market_Spend,
    SAFE_CAST(hexa_Spend AS FLOAT64) AS hexa_Spend,
    SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', Ingestion_time) AS Ingestion_time_DATETIME,
    YYYYMM
    from  {{ ref('stg_smi_nz_hexa_vs_totalmarket') }}
)

SELECT * FROM hexavstma
