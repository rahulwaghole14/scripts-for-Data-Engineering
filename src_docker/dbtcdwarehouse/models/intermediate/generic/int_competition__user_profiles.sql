{{
  config(
    tags=['competitions'],
    )
}}

WITH profiles AS (

    SELECT
    marketing_id
    , email
    , TO_JSON_STRING(ARRAY[STRUCT("2024_350190103_whats-got-your-goat-new-zealand" AS competition_entry)]) AS competition_entries -- noqa
    FROM {{ ref('stg_competition_whats_got_your_goat') }}
    WHERE confirmation LIKE '%I wish to receive emails with news and communications from hexa%'

)

SELECT marketing_id, email, ANY_VALUE(competition_entries) AS competition_entries
FROM profiles GROUP BY 1, 2 ORDER BY 1 ASC
