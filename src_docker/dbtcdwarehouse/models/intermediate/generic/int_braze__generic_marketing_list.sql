{{
  config(
        tags=['braze']
    )
}}

WITH all_subs AS (

    SELECT DISTINCT marketing_id FROM {{ ref('int_matrix__to_braze_user_profile') }}

)

, new_subs AS (

    SELECT
        customer.marketing_id
        , 'generic_marketing_newsletter_print-subscriber-marketing' AS internal_name
        , GREATEST(
            COALESCE(subscription.update_time, TIMESTAMP '1970-01-01'),
            COALESCE(subscriber.update_time, TIMESTAMP '1970-01-01'),
            COALESCE(customer.updated_at, TIMESTAMP '1970-01-01')
        ) AS updated_at
    FROM {{ ref('stg_matrix__bq_subscriptions') }} subscription
    INNER JOIN {{ ref('stg_matrix__bq_subscribers') }} subscriber ON subscription.subs_pointer = subscriber.subs_pointer
    INNER JOIN {{ ref('int_matrix__to_braze_user_profile') }} customer ON CAST(customer.subs_id AS INT64) = CAST(subscriber.subs_id AS INT64)
    WHERE
        (customer.marketing_id IS NOT NULL)
        AND CAST(customer.DoNotCall AS INT64) = 0 -- opted out exclude
        AND CAST(subscription.sord_startdate AS DATE) >= '2023-07-26'
)

, new_active_subs AS (

    SELECT
        customer.marketing_id
        , CASE
            WHEN subscription.ProductID = 'DPT' THEN 'the-post_marketing_newsletter_print-subscriber-benefits'
            WHEN subscription.ProductID = 'CHP' THEN 'the-press_marketing_newsletter_print-subscriber-benefits'
            WHEN subscription.ProductID = 'WKT' THEN 'waikato-times_marketing_newsletter_print-subscriber-benefits'
            WHEN subscription.ProductID = 'STL' THEN 'southland-times_marketing_newsletter_print-subscriber-benefits'
            WHEN subscription.ProductID = 'TDN' THEN 'taranaki-daily-news_marketing_newsletter_print-subscriber-benefits'
            WHEN subscription.ProductID = 'SST' THEN 'sunday-star-times_marketing_newsletter_print-subscriber-benefits'
            WHEN subscription.ProductID = 'SUN' THEN 'sunday-news_marketing_newsletter_print-subscriber-benefits'
            WHEN subscription.ProductID = 'NEL' THEN 'nelson-mail_marketing_newsletter_print-subscriber-benefits'
            WHEN subscription.ProductID = 'MEX' THEN 'marlborough-express_marketing_newsletter_print-subscriber-benefits'
            WHEN subscription.ProductID = 'MAN' THEN 'manawatu-standard_marketing_newsletter_print-subscriber-benefits'
            WHEN subscription.ProductID = 'TIM' THEN 'the-timaru-herald_marketing_newsletter_print-subscriber-benefits'
            WHEN subscription.ProductID = 'WTA' THEN 'wairarapa-times-age_marketing_newsletter_print-subscriber-benefits'
            ELSE 'Unknown'
        END AS internal_name
        , subscription.ProductID
        , CASE WHEN subord_cancel.sord_pointer IS NOT NULL AND CAST(subord_cancel.canx_date AS DATE) < CURRENT_DATE() THEN 0 ELSE 1 END AS is_active
        , subscription.sord_startdate
        , subscriber.subs_pointer -- this
        , GREATEST(
            COALESCE(subscription.update_time, TIMESTAMP '1970-01-01'),
            COALESCE(subscriber.update_time, TIMESTAMP '1970-01-01'),
            COALESCE(customer.updated_at, TIMESTAMP '1970-01-01'),
            COALESCE(subord_cancel.update_time, TIMESTAMP '1970-01-01')
        ) AS updated_at
    FROM {{ ref('stg_matrix__bq_subscriptions') }} subscription
    INNER JOIN {{ ref('stg_matrix__bq_subscribers') }} subscriber ON subscription.subs_pointer = subscriber.subs_pointer
    INNER JOIN {{ ref('int_matrix__to_braze_user_profile') }} customer ON CAST(customer.subs_id AS INT64) = CAST(subscriber.subs_id AS INT64)
    LEFT JOIN {{ ref('stg_matrix__bq_subord_cancel') }} subord_cancel
        ON
            subscription.sord_pointer = subord_cancel.sord_pointer
            AND subscription.ProductID = subord_cancel.ProductID
    WHERE
        (
            CAST(subscription.sord_stopdate AS DATE) >= CURRENT_DATE()
            OR subscription.sord_stopdate IS NULL
        )
        AND (CAST(subord_cancel.canx_date AS DATE) >= CURRENT_DATE() OR subord_cancel.canx_date IS NULL)
        AND (CAST(subscription.exp_end_date AS DATE) >= CURRENT_DATE() OR subscription.exp_end_date IS NULL)
        AND (customer.marketing_id IS NOT NULL)
        AND CAST(customer.DoNotCall AS INT64) = 0 -- opted out exclude
        AND CAST(subscription.sord_startdate AS DATE) >= '2023-07-26'

)

, blacklist as (
    select LOWER(marketing_id) AS marketing_id,
           MAX(updated_at) AS updated_at
    from {{ ref('stg_acm__print_data_blacklist') }}
    GROUP BY LOWER(marketing_id)
)

, active_sub_new_final AS (

    SELECT
        new_active_subs.marketing_id
        , new_active_subs.internal_name
        , new_active_subs.sord_startdate
        , new_active_subs.is_active
        , new_active_subs.ProductID
        , new_active_subs.subs_pointer
        , GREATEST(
            COALESCE(new_active_subs.updated_at, TIMESTAMP '1970-01-01'),
            COALESCE(bl.updated_at, TIMESTAMP '1970-01-01')
        ) AS updated_at
    FROM new_active_subs
    LEFT JOIN blacklist bl ON new_active_subs.marketing_id = bl.marketing_id
    WHERE
        bl.marketing_id IS NULL
        AND new_active_subs.internal_name != 'Unknown'

)

, presspatron_subscribers AS (

    SELECT DISTINCT
        braze_user_profiles.marketing_id AS marketing_id
        , 'hexa_supporter-programme_newsletter_supporter-monthly' AS internal_name
        , GREATEST(
            COALESCE(MAX(bl.updated_at), TIMESTAMP '1970-01-01'),
            COALESCE(MAX(braze_user_profiles.record_load_dts), TIMESTAMP '1970-01-01')
        ) AS updated_at
    FROM {{ ref('stg_presspatron__braze_user_profiles') }} braze_user_profiles
    LEFT JOIN blacklist bl -- exclude blacklist
        ON braze_user_profiles.marketing_id = bl.marketing_id
    WHERE
        bl.marketing_id IS NULL
        AND braze_user_profiles.Subscribed_to_newsletter = 'Yes'
    GROUP BY braze_user_profiles.marketing_id

)

, generic_blacklist AS (

    SELECT address AS email, marketing_id, updated_at
    FROM {{ ref('stg_acm__print_data_blacklist') }}
    WHERE CAST(status AS INT64) IN (3, 4)
    UNION ALL
    SELECT email, marketing_id_email AS marketing_id, updated_at
    FROM {{ ref('stg_idm__user_profiles') }}
    WHERE user_consent = 'False'

)

, generic_no_consent AS (

    SELECT marketing_id, MAX(updated_at) as updated_at
    FROM generic_blacklist
    GROUP BY marketing_id

)

, generic_list AS (

    SELECT email, marketing_id, updated_at
    FROM {{ ref('stg_acm__print_data_generic') }}
    WHERE internal_name = 'fairfaxMarketing'

    UNION ALL

    SELECT d.email
        , coalesce(a.marketing_id, d.marketing_id) AS marketing_id
        , GREATEST(
            COALESCE(CAST (a.updated_at AS TIMESTAMP), TIMESTAMP '1970-01-01'),
            COALESCE(d.updated_at, TIMESTAMP '1970-01-01'),
            COALESCE(g.updated_at, TIMESTAMP '1970-01-01')
        ) AS updated_at
    FROM {{ ref('stg_acm__print_data_generic') }} d
    INNER JOIN {{ ref('stg_idm__user_profiles') }} a ON d.marketing_id = a.marketing_id_email
    LEFT JOIN generic_no_consent g ON d.marketing_id = g.marketing_id
    WHERE
        internal_name = 'fairfaxMarketing'
        AND g.marketing_id IS null

    UNION ALL

    SELECT email
        , marketing_id AS marketing_id
        , CAST(record_load_dts_utc AS TIMESTAMP)  as updated_at
    FROM {{ ref('stg_idm__user_profiles') }}
    WHERE user_consent = 'True'

)

, generic_consent AS (

    SELECT marketing_id
        , MAX(updated_at) AS updated_at
        FROM generic_list
        GROUP BY marketing_id

)

, generic_final AS (

    SELECT
        generic_consent.marketing_id AS marketing_id
        , 'hexa_marketing_email_general-marketing-comms' AS internal_name
        , GREATEST(
            COALESCE(MAX(generic_consent.updated_at), TIMESTAMP '1970-01-01'),
            COALESCE(MAX(generic_no_consent.updated_at), TIMESTAMP '1970-01-01')
        ) AS updated_at
FROM generic_consent
    LEFT JOIN generic_no_consent ON (generic_consent.marketing_id = generic_no_consent.marketing_id)
    WHERE generic_no_consent.marketing_id IS null
    GROUP BY generic_consent.marketing_id

)

, union_all AS (

    SELECT
        marketing_id
        , internal_name
        , updated_at
    FROM generic_final

    UNION ALL

    SELECT
        marketing_id
        , internal_name
        , updated_at
    FROM active_sub_new_final
    WHERE active_sub_new_final.is_active = 1

    UNION ALL

    SELECT
        new_subs.marketing_id
        , 'generic_marketing_newsletter_print-subscriber-marketing' AS internal_name
        , new_subs.updated_at
    FROM
        new_subs
    LEFT JOIN blacklist bl
        ON new_subs.marketing_id = bl.marketing_id
    WHERE bl.marketing_id IS NULL

    UNION ALL

    SELECT
        marketing_id
        , internal_name
        , updated_at
    FROM {{ ref('stg_acm__print_generic_final') }}

    UNION ALL

    SELECT
        print_subbenefit_final.marketing_id
        , print_subbenefit_final.internal_name
        , GREATEST(
            COALESCE(active_sub_new_final.updated_at, TIMESTAMP '1970-01-01'),
            COALESCE(print_subbenefit_final.updated_at, TIMESTAMP '1970-01-01')
        ) AS updated_at
    FROM
        {{ ref('stg_acm__print_subbenefit_final') }} print_subbenefit_final
    LEFT JOIN active_sub_new_final
        ON
            print_subbenefit_final.marketing_id = active_sub_new_final.marketing_id
            AND print_subbenefit_final.product_id = active_sub_new_final.ProductID
    WHERE active_sub_new_final.ProductID IS null OR active_sub_new_final.is_active = 1

    UNION ALL

    SELECT  marketing_id
            , internal_name
            , updated_at
    FROM presspatron_subscribers

)

SELECT
    LOWER(marketing_id) AS marketing_id
    , internal_name
    , MAX(updated_at) AS updated_at
FROM union_all
GROUP BY LOWER(marketing_id), internal_name
