{{
  config(
    materialized = 'table',
    tags=['gam']
    )
}}

WITH list_new AS (

    {{ dbt_utils.unpivot(
        relation=ref('int_gam__am_audience_segments_new'),
        cast_to='boolean',
        field_name='LIST_NAME',
        value_name='IS_TRUE',
        exclude=['ppid']
    ) }}

)

, audience_ids AS (
    SELECT audience_id AS LIST_ID
      , CONCAT('GROUPM_', SUBSTR(REPLACE(UPPER(ORIGINAL_NAME), ' ', ''), 5)) AS LIST_NAME
    FROM {{ ref('groupm__new_audience_ids') }}
)

SELECT
    list_new.ppid AS PPID
    , audience_ids.LIST_ID
    -- row hash
    , MD5(CONCAT(list_new.ppid, audience_ids.LIST_ID)) AS AUDIENCE_SEGMENT_HASH
FROM list_new
JOIN audience_ids USING (LIST_NAME)
WHERE
    list_new.IS_TRUE = TRUE
    AND list_new.PPID IS NOT NULL
ORDER BY
    1, 2
