{{
config(
    tags='bq_matrix'
)
}}

WITH customers AS (
    SELECT * FROM {{ ref('int_matrix__to_braze_user_profile') }}
)

, communication_subscriptions AS (
    SELECT * FROM {{ ref('int_matrix__to_braze__matrix_user_profiles_print_communication_subscriptions') }}
)

, piano_subscriptions AS (
    SELECT
        marketing_id -- possible match ON
        , LOWER(TRIM(user.email)) AS email -- possible match ON
    FROM
        {{ ref('stg_piano__subscriptions') }}
    WHERE
        LOWER(status) = 'active'
        AND (end_date IS NULL OR DATE(TIMESTAMP_SECONDS(end_date)) > CURRENT_DATE('Pacific/Auckland'))
)

, matrix_subscription_details AS (

    SELECT *
        , CASE WHEN ProductID IN ('NZG', 'NZHG', 'TVG', 'WTA')
            AND active_subscription = 1 THEN 1 ELSE 0 END AS is_active_magazine_subscription
        , CASE WHEN ServiceID IN ('ANY-1', 'SUN-ONLY', 'ONLY-SAT', 'FRI-ONLY', 'THUR-ONLY', 'TUE-ONLY', 'WED-ONLY')
            AND active_subscription = 1 THEN 1 ELSE 0 END AS is_discount_eligible
        , CASE WHEN ServiceID IN ('ANY-2', 'ANY-3', 'MON&SAT', 'MON-FRI', 'MON-SAT', 'MONWEDFRI', 'MONWEDSAT', 'MWF&S', 'TUE&THURS', 'TUETHUSAT','TUE-SAT','TUE-FRI')
            AND active_subscription = 1 THEN 1 ELSE 0 END AS is_free_eligible
        , CASE WHEN canx_id = 'DECEASED' THEN 1 ELSE 0 END AS is_deceased
    FROM {{ ref('int_matrix__subscription_details') }}

)

, subscribers AS (
    SELECT
        customers.hash_key AS primary_key
        , LOWER(customers.marketing_id) AS marketing_id
        , customers.subs_id AS print__sub_id -- USE THE MOST RECENT SUBSCRIPTION
        , subscribers.subs_pointer
        , LOWER(customers.subtype_id) AS print__sub_type
        , customers.FirstName AS first_name
        , customers.SurName AS last_name
        , customers.mobile
        , customers.home
        , customers.work
        , customers.title
        , CASE
            WHEN customers.country_name = 'NEW ZEALAND' THEN 'NZ'
            WHEN customers.country_name = 'AUSTRALIA' THEN 'AU'
            ELSE NULL
        END AS country
        , customers.PersContact3 AS email -- Change this part as the email address has pre processed in the int_matrix__to_braze_user_profile, and for the one who has matrix email address only, we should not use the idm user email
        , CASE WHEN customers.has_matrix_or_idm_attributes = 1 THEN RIGHT(locations.AddrLine6, 4) ELSE NULL END AS print__postcode
        , CASE
            WHEN piano_subscriptions.email IS NOT NULL THEN 1
            ELSE 0
        END AS is_digital_masthead_subscriber
        , customers.DoNotCall AS print__do_not_contact
        , CASE WHEN  drupal__user_profiles.subscriber_id IS NOT NULL THEN  1 ELSE 0 END AS has_idm_am_account,
        drupal__user_profiles.user_id AS hexa_account_id
        , customers.updated_at
    FROM customers
    LEFT JOIN piano_subscriptions
        ON piano_subscriptions.email = LOWER(TRIM(customers.PersContact3))
    LEFT JOIN {{ ref('stg_idm__user_profiles') }} drupal__user_profiles
        ON customers.subs_id = drupal__user_profiles.subscriber_id
    LEFT JOIN {{ ref('stg_matrix__bq_subscribers') }} subscribers
        ON customers.subs_id = subscribers.subs_id
    LEFT JOIN {{ ref('stg_matrix__bq_persons') }} persons
        ON subscribers.subs_perid = persons.person_pointer
    LEFT JOIN {{ ref('stg_matrix__bq_locations') }} locations
        ON persons.ObjectPointer = locations.Level1Pointer
    {# WHERE drupal__user_profiles.subscriber_id IS NULL #}
)

, rate_code AS (
    SELECT
    DISTINCT
        subs_id AS print__sub_id
        , CASE
            WHEN Rate_head_id LIKE '%FREE%' THEN 1
            ELSE 0
        END
            AS print__has_free_rate_code -- boolean
        , round_id AS print__round_id
    FROM {{ ref('stg_matrix__bq_rpt_subscribers_rate_rounds') }}
)

, sub_active AS (
    SELECT
        s.primary_key
        , s.marketing_id
        , s.print__sub_id -- USE THE MOST RECENT SUBSCRIPTION
        , s.subs_pointer
        , s.print__sub_type
        , s.first_name
        , s.last_name
        , s.mobile
        , s.home
        , s.work
        , s.country
        , s.email
        , s.print__postcode
        , s.is_digital_masthead_subscriber
        , s.print__do_not_contact
        , s.has_idm_am_account
        , s.title
        , matrix_subscription_details.is_free_eligible AS free_eligible
        , matrix_subscription_details.is_discount_eligible AS discount_eligible
        , matrix_subscription_details.is_active_magazine_subscription AS active_magazine_subscription
        , CASE
            WHEN EXTRACT(MONTH FROM matrix_subscription_details.sord_stopdate) = EXTRACT(MONTH FROM DATE_ADD(CURRENT_DATE('Pacific/Auckland'), INTERVAL 1 MONTH))
                AND matrix_subscription_details.is_discount_eligible = 1 THEN 1
            WHEN EXTRACT(MONTH FROM matrix_subscription_details.exp_end_date) = EXTRACT(MONTH FROM DATE_ADD(CURRENT_DATE('Pacific/Auckland'), INTERVAL 1 MONTH))
                AND matrix_subscription_details.is_discount_eligible = 1 THEN 1
            WHEN EXTRACT(MONTH FROM matrix_subscription_details.canx_date) = EXTRACT(MONTH FROM DATE_ADD(CURRENT_DATE('Pacific/Auckland'), INTERVAL 1 MONTH))
                AND matrix_subscription_details.is_discount_eligible = 1 THEN 1
            ELSE 0
        END AS print__subscription_expiry_date_next_month
        , matrix_subscription_details.is_deceased AS print__deceased
        , CASE WHEN matrix_subscription_details.subs_pointer IS NOT NULL THEN 1 ELSE 0 END AS print__active_subscription
        , matrix_subscription_details.ProductID AS print__product
        , r.print__has_free_rate_code AS print__has_free_rate_code
        , r.print__round_id AS print__round_id
        {# determine the row number for the most recent subscription
        for the marketing_id because there can be multiple subscriptions with same marketing_id
        also order by customer hash_key to make it deterministic #}
        , ROW_NUMBER() OVER (PARTITION BY s.marketing_id ORDER BY matrix_subscription_details.last_touch_date desc, s.primary_key) AS row_number
        , matrix_subscription_details.last_touch_date
        , s.hexa_account_id
        , s.updated_at
    FROM subscribers s
    LEFT JOIN matrix_subscription_details ON s.subs_pointer = matrix_subscription_details.subs_pointer AND matrix_subscription_details.active_subscription = 1
    LEFT JOIN rate_code r ON s.print__sub_id = r.print__sub_id
)

, latest_sub AS (
    SELECT marketing_id as primary_key,
        marketing_id,
        print__sub_id,
        print__sub_type,
        first_name,
        last_name,
        mobile,
        home,
        work,
        country,
        email,
        print__postcode,
        title,
        hexa_account_id,
        updated_at
FROM sub_active
    WHERE row_number = 1
)

, aggregated_sub AS (

    SELECT
        marketing_id,
        MAX(free_eligible) AS free_eligible,
        MAX(discount_eligible) AS discount_eligible,
        MAX(active_magazine_subscription) AS active_magazine_subscription,
        MAX(print__subscription_expiry_date_next_month) AS print__subscription_expiry_date_next_month,
        MAX(is_digital_masthead_subscriber) AS is_digital_masthead_subscriber,
        MAX(print__active_subscription) AS print__active_subscription,
        MAX(print__deceased) AS print__deceased,
        MAX(print__has_free_rate_code) AS print__has_free_rate_code,
        MAX(print__round_id) AS print__round_id,
        CASE
            WHEN STRING_AGG(CAST(print__product AS STRING), ',') IS NULL THEN '[]'
            ELSE CONCAT('[', STRING_AGG(DISTINCT '"' || CAST(print__product AS STRING) || '"', ',' ORDER BY '"' || CAST(print__product AS STRING) || '"' ASC), ']')
        END AS print__product
        ,        CASE
            WHEN MAX(free_eligible) = 1 THEN 'free'
            WHEN MAX(discount_eligible) = 1 THEN 'discount'
            ELSE NULL
        END AS print__sub_digi_eligibility,
        CASE
            WHEN MAX(print__deceased) = 1 THEN '1'
            ELSE CAST(MAX(print__do_not_contact) AS STRING)
        END AS print__do_not_contact
        , MAX(has_idm_am_account) as has_idm_am_account
    FROM sub_active
    GROUP BY marketing_id

)

SELECT
    l.primary_key,
    l.marketing_id,
    l.print__sub_id,
    l.print__sub_type,
    l.first_name,
    l.last_name,
    l.mobile,
    l.home,
    l.work,
    l.country,
    l.email,
    l.print__postcode,
    l.title,
    l.hexa_account_id,
    GREATEST(
        COALESCE(l.updated_at, TIMESTAMP '1970-01-01'),
        COALESCE(c.updated_at, TIMESTAMP '1970-01-01')
    ) AS updated_at,
    a.print__sub_digi_eligibility,
    a.print__subscription_expiry_date_next_month,
    a.active_magazine_subscription,
    a.is_digital_masthead_subscriber,
    a.print__active_subscription,
    a.has_idm_am_account,
    a.print__do_not_contact,
    a.print__has_free_rate_code,
    a.print__round_id,
    a.print__product,
    a.print__deceased,
    c.print_communication_subscriptions
FROM latest_sub l
JOIN aggregated_sub a ON l.marketing_id = a.marketing_id
LEFT OUTER JOIN communication_subscriptions c ON l.marketing_id = c.marketing_id
