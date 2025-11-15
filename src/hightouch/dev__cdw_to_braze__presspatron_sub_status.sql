-- dev__cdw_to_braze__presspatron_sub_status
SELECT lower(hash_key) as hash_key  -- pk
    , lower(marketing_id) as marketing_id
    , case when [Subscribed to newsletter?] = 'Yes' then 'subscribed'
    else 'unsubscribed' end as subscription_status
FROM [stage].[presspatron].[braze_user_profiles]
