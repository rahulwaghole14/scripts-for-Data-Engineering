select
    top(20000) -- limit for dev
    -- get custom fields
    lower(marketing_id) as external_id  -- pk
    , user_id as hexa_account_id -- needs to be user_alias
    , subscriber_id as print__sub_id -- needs to be user_alias
    , CASE WHEN country = 'New Zealand' then 'NZ'
      when country = 'United States' then 'US'
      when country = 'Australia' then 'AU'
      else null end as country
    , lower(trim(email)) as email
    , last_name
    , first_name
    , contact_phone as phone
    , city_region as home_city
    , CASE
        WHEN gender IN ('M', 'Male', 'Man') THEN 'M'
        WHEN gender IN ('F', 'Female', 'Woman') THEN 'F'
        WHEN gender IN ('O', 'Non-binary', 'Takatapui') THEN 'O'
        WHEN gender IN ('N', '-1') THEN 'N'
        WHEN gender IN ('P', 'Rather not say', 'Rather-Not-Say') THEN 'P'
        WHEN gender IS NULL OR gender = 'U' THEN NULL
        ELSE NULL END as gender
    , timezone as time_zone
    , CONVERT(varchar, date_of_birth, 23) AS dob
    -- , postcode
    -- , display_name as digital__display_name
    , username as digital__username
    -- , date_of_birth
    -- , CONVERT(varchar, created_date, 127) AS digital__created_date
    -- , CONVERT(varchar, verified_date, 127) AS digital__email_verified_at
    -- , CONVERT(varchar, mobile_verified_date, 127) AS digital__mobile_verified_at
    -- , locality as digital__locality
    -- , case when active = 'True' then 1 else 0 end as digital__active -- boolean convert
    -- , case when user_consent = 'True' then 1 else 0 end as digital__consent -- boolean convert
    -- , case when email_verified = 'True' then 1 else 0 end as digital__email_verified -- boolean convert
    -- , case when mobile_verified = 'True' then 1 else 0 end as digital__mobile_verified -- boolean convert
from stage.idm.drupal__user_profiles
-- limit for dev
where newsletter_subs is not null
;
