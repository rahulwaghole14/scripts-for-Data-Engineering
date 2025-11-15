{{
  config(
    materialized = 'table',
    tags = 'adw'
    )
}}

-- models/dim_date.sql
WITH date_range AS (
    SELECT
        DATE_ADD(DATE '2000-01-01', INTERVAL seq DAY) AS date
    FROM
        UNNEST(GENERATE_ARRAY(0, DATE_DIFF(DATE '2030-12-31', DATE '2000-01-01', DAY))) AS seq
)

, week_numbers AS (
    SELECT
        date
        , CASE
            WHEN EXTRACT(DAYOFWEEK FROM DATE_TRUNC(date, YEAR)) = 1 THEN 0
            ELSE 1
        END
        + DATE_DIFF(date, DATE_TRUNC(date, YEAR), WEEK (SUNDAY)) AS us_week_number
        , CASE EXTRACT(DAYOFWEEK FROM date)
            WHEN 7 THEN 1  -- Sunday
            ELSE EXTRACT(DAYOFWEEK FROM date) + 1  -- Other days
        END AS us_day_of_week
    FROM date_range
)

SELECT
    FORMAT_DATE('%Y%m%d', d.date) AS DimDateKey
    , EXTRACT(YEAR FROM d.date) AS CalYearKey
    , CONCAT('Cal-', CAST(EXTRACT(YEAR FROM d.date) AS STRING)) AS CalYearName
    , CONCAT(EXTRACT(YEAR FROM d.date), LPAD(CAST(EXTRACT(QUARTER FROM d.date) AS STRING), 2, '0')) AS CalYearQuarterKey
    , EXTRACT(QUARTER FROM d.date) AS CalQuarterKey
    , CONCAT('Q', CAST(EXTRACT(QUARTER FROM d.date) AS STRING), '-', FORMAT_DATE('%y', d.date)) AS CalQuarterName
    , CONCAT(EXTRACT(YEAR FROM d.date), LPAD(CAST(EXTRACT(MONTH FROM d.date) AS STRING), 2, '0')) AS CalYearMonthKey
    , EXTRACT(MONTH FROM d.date) AS CalMonthKey
    , FORMAT_DATE('%Y-%m-%d', DATE_TRUNC(d.date, MONTH)) AS CalYearMonthName
    -- Calendar Week Key (ISO Year and Week Number)
    , us_week_number AS CalWeekKey
    , CONCAT('Week ', us_week_number) AS CalWeekName
    , CONCAT(FORMAT_DATE('%G', d.date), us_week_number) AS CalYearWeekKey
    -- Calendar Year-Week Name
    , CONCAT(FORMAT_DATE('%G', d.date), ' Week ', us_week_number) AS CalYrWeekName

    -- Calendar Day of Month
    , CAST(FORMAT_DATE('%d', d.date) AS INT64) AS CalDayOfMonth

    -- Calendar Day of Week Key (ISO Day of Week)
    , EXTRACT(WEEK FROM d.date) AS CalDayOfWeekKey

    -- Fiscal Year Key (assuming fiscal year starting in July)
    , CASE
        WHEN EXTRACT(MONTH FROM d.date) >= 7 THEN EXTRACT(YEAR FROM d.date) + 1
        ELSE EXTRACT(YEAR FROM d.date)
    END AS FinYearKey

    -- Fiscal Year Name
    , CONCAT(
        'FY-'
        , CASE
            WHEN EXTRACT(MONTH FROM d.date) >= 7 THEN EXTRACT(YEAR FROM d.date) + 1
            ELSE EXTRACT(YEAR FROM d.date)
        END
    ) AS FinYearName

    -- Fiscal Year Alt Name
    , CONCAT(
        'F-'
        , CASE
            WHEN EXTRACT(MONTH FROM d.date) >= 7 THEN EXTRACT(YEAR FROM d.date) + 1
            ELSE EXTRACT(YEAR FROM d.date)
        END
    ) AS FinYearAltName

    -- Fiscal Year Quarter Key
    , CAST(
        CONCAT(
            CAST(
                CASE
                    WHEN EXTRACT(MONTH FROM d.date) >= 7 THEN EXTRACT(YEAR FROM d.date) + 1
                    ELSE EXTRACT(YEAR FROM d.date)
                END AS STRING
            )
            , LPAD(
                CAST(
                    FLOOR((MOD(EXTRACT(MONTH FROM d.date) + 5, 12) / 3 + 1)) AS STRING
                )
                , 2
                , '0'
            )
        ) AS INT64
    ) AS FinYearQuarterKey
    -- Fiscal Quarter Key
    , CAST(
        (MOD(EXTRACT(MONTH FROM d.date) + 5, 12) / 3 + 1) AS INT64
    ) AS FinQuarterKey

    -- Fiscal Quarter Name
    , CONCAT(
        'Q'
        , CAST(
            (MOD(EXTRACT(MONTH FROM d.date) + 5, 12) / 3 + 1) AS STRING
        )
        , ' F'
        , RIGHT(
            CAST(
                CASE
                    WHEN EXTRACT(MONTH FROM d.date) >= 7 THEN EXTRACT(YEAR FROM d.date) + 1
                    ELSE EXTRACT(YEAR FROM d.date)
                END AS STRING
            )
            , 2
        )
    )
        AS FinQuarterName

    , CAST(CONCAT(
        CASE
            WHEN EXTRACT(MONTH FROM d.date) >= 7 THEN EXTRACT(YEAR FROM d.date) + 1
            ELSE EXTRACT(YEAR FROM d.date)
        END
        , LPAD(CAST(MOD(EXTRACT(MONTH FROM d.date) + 6, 12) AS STRING), 2, '0')
    ) AS INT64) AS FinYearMonthKey

    -- Fiscal Month Key (simple month number in fiscal year, assuming July start)
    , CAST(MOD(EXTRACT(MONTH FROM d.date) + 6, 12) AS INT64) AS FinMonthKey

    -- Fiscal Month Name
    , FORMAT_DATE('%B', d.date) AS FinMonthName

    -- Fiscal Year-Week Key
    , CAST(CONCAT(
        CASE
            WHEN EXTRACT(MONTH FROM d.date) >= 7 THEN EXTRACT(YEAR FROM d.date) + 1
            ELSE EXTRACT(YEAR FROM d.date)
        END
        , LPAD(CAST(CAST(EXTRACT(WEEK FROM DATE_ADD(d.date, INTERVAL -DATE_DIFF(DATE_TRUNC(d.date, YEAR), DATE '1900-07-01', WEEK) WEEK)) AS INT64) + 1 AS STRING), 2, '0')
    ) AS INT64) AS FinYearWeekKey

    -- Fiscal Week Key (assuming the first week of the fiscal year starts on July 1)
    , CAST(EXTRACT(WEEK FROM DATE_ADD(d.date, INTERVAL -DATE_DIFF(DATE_TRUNC(d.date, YEAR), DATE '1900-07-01', WEEK) WEEK)) AS INT64) + 1 AS FinWeekKey

    -- Fiscal Week Name
    , CONCAT('Week ', LPAD(CAST(CAST(EXTRACT(WEEK FROM DATE_ADD(d.date, INTERVAL -DATE_DIFF(DATE_TRUNC(d.date, YEAR), DATE '1900-07-01', WEEK) WEEK)) AS INT64) + 1 AS STRING), 2, '0')) AS FinWeekName

    -- Fiscal Year-Week Name
    , CONCAT(
        CASE
            WHEN EXTRACT(MONTH FROM d.date) >= 7 THEN EXTRACT(YEAR FROM d.date) + 1
            ELSE EXTRACT(YEAR FROM d.date)
        END, ' week '
        , LPAD(CAST(CAST(EXTRACT(WEEK FROM DATE_ADD(d.date, INTERVAL -DATE_DIFF(DATE_TRUNC(d.date, YEAR), DATE '1900-07-01', WEEK) WEEK)) AS INT64) + 1 AS STRING), 2, '0'))
        AS FinYearWeekName

    -- Fiscal Day of Week Key
    , EXTRACT(DAYOFWEEK FROM d.date) AS FinDayOfWeekKey

    -- Generic Month Name
    , FORMAT_DATE('%B', d.date) AS GenMonthName

    -- Generic Weekday Name
    , FORMAT_DATE('%A', d.date) AS GenWeekDayName

    -- Generic Weekday d.date Name
    , CONCAT(FORMAT_DATE('%a', d.date), ' ', FORMAT_DATE('%e', d.date), ' ', FORMAT_DATE('%b', d.date), ' ', FORMAT_DATE('%Y', d.date))
        AS GenWeekDayDateName

    -- Generic d.date Name
    , FORMAT_DATE('%B %e %Y', d.date) AS GenDateName
    , d.date AS GenDate
FROM
    date_range d
LEFT JOIN week_numbers w
    ON d.date = w.date
