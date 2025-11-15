-- dev__cdw_to_braze__presspatron_user_profiles
SELECT
    -- get all fields
    -- get custom data types
    hash_key as primary_key  -- pk
    , lower(marketing_id) as marketing_id
    , [First name] as first_name
    , [Last name] as last_name
    , [Email address] as email
    , CASE
    WHEN [Subscribed to newsletter?] = 'Yes' THEN 1
    WHEN [Subscribed to newsletter?] = 'No' THEN 0
    ELSE NULL
    END as supporter__subscribed_to_newsletter
    , CASE
    WHEN [Active supporter?] = 'Yes' THEN 1
    WHEN [Active supporter?] = 'No' THEN 0
    ELSE NULL
    END as supporter__active
    , FORMAT(CAST([Sign up date] AS DATETIME), 'yyyy-MM-dd') AS supporter__sign_up_date
    -- , NULL AS supporter__sign_up_date
    , [Total contributions to date] as supporter__total_contribution
    , [Recurring contribution amount?] as supporter__recurring_contribution
    , [Frequency of recurring payment] as supporter__frequency
FROM [stage].[presspatron].[braze_user_profiles]
-- where marketing_id = '05104569-3fa8-5ac0-a001-2ecd91dc49ba'
-- where [Subscribed to newsletter?] = 'No'

-- select
--     count(distinct user_id), FORMAT(CONVERT(datetimeoffset, created_date), 'yyyy-MM-dd') AS FormattedDate
-- from stage.idm.drupal__user_profiles
-- -- limit for dev
-- where newsletter_subs is not null
-- group by FORMAT(CONVERT(datetimeoffset, created_date), 'yyyy-MM-dd')
-- order by 2
