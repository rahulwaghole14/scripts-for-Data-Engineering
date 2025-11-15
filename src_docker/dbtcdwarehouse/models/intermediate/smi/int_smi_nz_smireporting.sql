{{ config(
    tags=['smi'],
    materialized='table',
    on_schema_change='sync_all_columns'
) }}


WITH nz_smi AS (
    SELECT
        Media,
        MasterMediaType,
        MediaType,
        MediaSubType,
        Country,
        State,
        MarketType,
        Market,
        Network,
        MasterOwner,
        Owner,
        SalesRep,
        TargetAudience,
        Genre,
        YearMonth,
        -- Concatenate '01' to YearMonth and cast it to DATETIME
        SAFE.PARSE_DATETIME('%Y%m%d', CONCAT(CAST(YearMonth AS STRING), '01')) AS YearMonth_DATETIME,
        -- Cast GrossMediaSpend to FLOAT64
        SAFE_CAST(GrossMediaSpend AS FLOAT64) AS GrossMediaSpend,
        CalYear,
        {{ adapter.quote("CalYear-Quarter") }} AS CalYear_Quarter,
        {{ adapter.quote("CalYear-Month") }} AS CalYear_Month,
        CalQuarter,
        CalMonth,
        MasterNetwork,
        SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', Ingestion_time) AS Ingestion_time_DATETIME,
        YYYYMM

    FROM {{ ref('stg_smi_nz_smireporting') }}
)
SELECT * FROM nz_smi
