{{
  config(
        tags=['hexa_google_analytics']
    )
}}

WITH MAX_TIMESTAMP AS (
    SELECT MAX(event_time) AS max_event_timestamp FROM (
        SELECT TIMESTAMP_MICROS(event_timestamp) AS event_time
        FROM {{ ref('stg_hexa_google_analytics__events_fresh_yesterday') }}
        UNION ALL SELECT TIMESTAMP_MICROS(event_timestamp) AS event_time
        FROM {{ ref('stg_hexa_google_analytics__events_intraday') }} 
        WHERE TIMESTAMP_MICROS(event_timestamp) <= current_timestamp()
  )
)

, EVENTS_FRESH_YESTERDAY AS (

    SELECT REGEXP_EXTRACT(EVENT_PARAMS.value.string_value, r'\d+') AS article_id
    , COUNT(1) AS pageviews
    FROM {{ ref('stg_hexa_google_analytics__events_fresh_yesterday') }}
     , UNNEST(event_params) AS EVENT_PARAMS
    JOIN MAX_TIMESTAMP ON TIMESTAMP_MICROS(event_timestamp) >= TIMESTAMP_SUB(max_event_timestamp, INTERVAL 30 MINUTE)
    WHERE EVENT_PARAMS.key = 'page_location'
    AND event_name ='page_view'
    AND REGEXP_CONTAINS(EVENT_PARAMS.value.string_value, r'\d+')
    GROUP BY 1

)

, EVENTS_INTRADAY_TODAY AS (

    SELECT REGEXP_EXTRACT(EVENT_PARAMS.value.string_value, r'\d+') AS article_id
    , COUNT(1) AS pageviews
    FROM {{ ref('stg_hexa_google_analytics__events_intraday') }}
     , UNNEST(event_params) AS EVENT_PARAMS
    JOIN MAX_TIMESTAMP ON TIMESTAMP_MICROS(event_timestamp) >= TIMESTAMP_SUB(max_event_timestamp, INTERVAL 30 MINUTE)
    WHERE EVENT_PARAMS.key = 'page_location'
    AND event_name ='page_view'
    AND REGEXP_CONTAINS(EVENT_PARAMS.value.string_value, r'\d+')
    GROUP BY 1

)

, MERGE_DATA AS (

    SELECT article_id
    , SUM(pageviews) AS pageviews
    FROM (
        SELECT * FROM EVENTS_FRESH_YESTERDAY
        UNION ALL SELECT * FROM EVENTS_INTRADAY_TODAY
    )
    GROUP BY 1
    ORDER BY 2 DESC

)

SELECT * FROM MERGE_DATA
