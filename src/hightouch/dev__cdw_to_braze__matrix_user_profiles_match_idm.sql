-- use stage;
-- its just the same query but exlclude the user standard fields i.e. email to not overwrite idm one

-- -- after create view use hightouch query

-- hightouch name: {dev|prod}__cdw_to_braze__matrix_user_profiles_match_idm

-- SELECT TOP (20000) [primary_key]
--       ,lower([marketing_id]) as marketing_id
--       ,[print__sub_id]
--       ,[print__sub_type]
--       ,[first_name]
--       ,[last_name]
--       ,[phone]
--       ,[country]
--       ,[email]
--       ,[print__postcode]
--       ,[print__sub_digi_eligibility]
--       ,[print__subscription_expiry_date_next_month] -- {{ row['print__subscription_expiry_date_next_month'] | cast : 'boolean' }}
--       ,[active_magazine_subscription] -- {{ row['active_magazine_subscription'] | cast : 'boolean' }}
--       ,[is_digital_masthead_subscriber] -- {{ row['is_digital_masthead_subscriber'] | cast : 'boolean' }}
--       ,[print__active_subscription] -- {{ row['print__active_subscription'] | cast : 'boolean' }}
--       ,[has_idm_am_account] -- {{ row['has_idm_am_account'] | cast : 'boolean' }}
--       ,[print__do_not_contact] -- {{ row['print__do_not_contact'] | cast : 'boolean' }}
--       ,[print__product]  -- {{ row['print__product'] | parse }}
--       ,[print__round_id]
--       ,[print__has_free_rate_code] -- {{ row['print__has_free_rate_code'] | cast : 'boolean' }}
--   FROM [stage].[matrix].[hightouch_user_profiles_match_idm]
--   where marketing_id is not null
--   and marketing_id = '4d79feb0-81e8-5ad6-b2f3-0d173c9a0180'



USE STAGE;

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- EXEC stage.matrix.load_braze_user_profiles_match_idm

CREATE OR ALTER PROCEDURE [matrix].[load_braze_user_profiles_match_idm]
AS
BEGIN

    ;
    IF EXISTS (SELECT * FROM sys.tables WHERE name = 'hightouch_user_profiles_match_idm' AND schema_id = SCHEMA_ID('matrix'))
    DROP TABLE stage.matrix.hightouch_user_profiles_match_idm;

    ;

    WITH piano_subs
    AS (
        SELECT marketing_id -- possible match on
            ,lower(trim(user__email)) AS email -- possible match on
        FROM [stage].[piano].[piano__subscriptions]
        WHERE lower(sub__status) = 'active'
            AND (
                sub__end_date IS NULL
                OR DATEADD(SECOND, sub__end_date, '1970-01-01') > GETUTCDATE()
                )
        )
        ,active_subs
    AS (
        SELECT DISTINCT subscription.subs_pointer
            ,subscription.ServiceID
            ,subscription.ProductID
            ,subscription.period_id
            ,subscription.sord_stopdate
            ,subscription.exp_end_date
            ,subord_cancel.canx_date
            ,subord_cancel.canx_id
        FROM stage.matrix.subscription
        LEFT JOIN stage.matrix.subord_cancel ON subord_cancel.sord_pointer = subscription.sord_pointer
            AND subord_cancel.ProductID = subscription.ProductID
        WHERE (
                subscription.sord_stopdate >= CAST(GETDATE() AS DATE)
                OR subscription.sord_stopdate IS NULL
                )
            AND (
                subord_cancel.canx_date >= CAST(GETDATE() AS DATE)
                OR subord_cancel.canx_date IS NULL
                )
            AND (
                subscription.exp_end_date >= CAST(GETDATE() AS DATE)
                OR subscription.exp_end_date IS NULL
                )
        )
        ,subscribers
    AS (
        SELECT c.hash_key AS primary_key
            ,lower(c.marketing_id) AS marketing_id
            ,c.subs_id AS print__sub_id
            ,s.subs_pointer
            ,lower(c.subs_type) AS print__sub_type
            ,c.FirstName AS first_name
            ,c.SurName AS last_name
            ,coalesce(c.mobile, c.home, c.WORK) AS phone
            ,CASE 
                WHEN c.CountryName = 'NEW ZEALAND'
                    THEN 'NZ'
                WHEN c.CountryName = 'AUSTRALIA'
                    THEN 'AU'
                ELSE NULL
                END AS country
            ,lower(trim(coalesce(i.email, c.PersContact3))) AS email
            ,right(tl.AddrLine6, 4) AS print__postcode
            ,CASE 
                WHEN ps.email IS NOT NULL
                    THEN 1
                ELSE 0
                END AS is_digital_masthead_subscriber
            ,c.DoNotCall AS print__do_not_contact
        FROM [stage].[matrix].[customer] c
        LEFT JOIN piano_subs ps ON ps.email = lower(trim(c.PersContact3))
        LEFT JOIN stage.idm.drupal__user_profiles i ON i.subscriber_id = c.subs_id
        LEFT JOIN stage.matrix.subscriber s ON s.subs_id = c.subs_id
        LEFT JOIN stage.matrix.tbl_person t ON t.person_pointer = s.subs_perid
        LEFT JOIN stage.matrix.tbl_location tl ON tl.Level1Pointer = t.ObjectPointer
        -- WHERE DoNotCall = 0
        WHERE i.subscriber_id IS NOT NULL -- need to seperate has because of marketing_id clash (email)
        )
        ,rate_code
    AS (
        SELECT DISTINCT sub_id AS print__sub_id
            ,CASE 
                WHEN rateitemid LIKE '%FREE%'
                    THEN 1
                ELSE 0
                END AS print__has_free_rate_code -- boolean
            ,RoundID AS print__round_id
        FROM stage.matrix.active_subscriber_all
        )
        ,sub_active
    AS (
        SELECT s.*
            ,CASE 
                WHEN a.ServiceID IS NOT NULL
                    THEN 1
                ELSE 0
                END AS discount_eligible
            ,CASE 
                WHEN a2.ServiceID IS NOT NULL
                    THEN 1
                ELSE 0
                END AS free_eligible
            ,CASE 
                WHEN a1.ProductID IS NOT NULL
                    THEN 1
                ELSE 0
                END AS active_magazine_subscription
            ,CASE 
                WHEN DATEPART(month, a.sord_stopdate) = DATEPART(month, DATEADD(month, 1, GETDATE()))
                    THEN 1
                WHEN DATEPART(month, a.exp_end_date) = DATEPART(month, DATEADD(month, 1, GETDATE()))
                    THEN 1
                WHEN DATEPART(month, a.canx_date) = DATEPART(month, DATEADD(month, 1, GETDATE()))
                    THEN 1
                ELSE 0
                END AS print__subscription_expiry_date_next_month
            ,CASE 
                WHEN a7.canx_id = 'DECEASED'
                    THEN 1
                ELSE 0
                END AS print__deceased
            ,CASE 
                WHEN a7.subs_pointer IS NOT NULL
                    THEN 1
                ELSE 0
                END AS print__active_subscription
            ,a7.ProductID AS print__product
            ,r.print__has_free_rate_code AS print__has_free_rate_code
            ,r.print__round_id AS print__round_id
        -- , case when i.subscriber_id is not null then 1 else 0 end as has_idm_account -- for the matrix user_profiles without email sync in hightouch
        FROM subscribers s
        LEFT JOIN active_subs a7 ON a7.subs_pointer = s.subs_pointer
        -- left join idm_data i on i.subscriber_id = s.print__sub_id -- for the matrix user_profiles without email sync in hightouch
        LEFT JOIN active_subs a1 ON a1.subs_pointer = s.subs_pointer -- magazine
            AND a1.ProductID IN (
                'NZG'
                ,'NZHG'
                ,'TVG'
                )
        LEFT JOIN active_subs a ON a.subs_pointer = s.subs_pointer -- discount
            AND a.ServiceID IN (
                'ANY-1'
                ,'SUN-ONLY'
                ,'ONLY-SAT'
                ,'FRI-ONLY'
                ,'THUR-ONLY'
                ,'TUE-ONLY'
                ,'WED-ONLY'
                )
        LEFT JOIN active_subs a2 ON a2.subs_pointer = s.subs_pointer -- free
            AND a2.ServiceID IN (
                'ANY-2'
                ,'ANY-3'
                ,'MON&SAT'
                ,'MON-FRI'
                ,'MON-SAT'
                ,'MONWEDFRI'
                ,'MONWEDSAT'
                ,'MWF&S'
                ,'TUE&THURS'
                ,'TUETHUSAT'
                )
        LEFT JOIN rate_code r ON r.print__sub_id = s.print__sub_id
        )
    SELECT primary_key
        ,marketing_id
        ,print__sub_id
        ,print__sub_type
        ,first_name
        ,last_name
        ,phone
        ,country
        ,email
        ,-- exclude because it matches idm
        print__postcode
        ,CASE 
            WHEN MAX(free_eligible) = 1
                THEN 'free'
            WHEN MAX(discount_eligible) = 1
                THEN 'discount'
            ELSE NULL
            END AS print__sub_digi_eligibility
        ,MAX(print__subscription_expiry_date_next_month) AS print__subscription_expiry_date_next_month
        ,MAX(active_magazine_subscription) AS active_magazine_subscription
        ,MAX(is_digital_masthead_subscriber) AS is_digital_masthead_subscriber
        ,MAX(print__active_subscription) AS print__active_subscription
        ,1 AS has_idm_am_account
        ,CASE 
            WHEN print__deceased = 1
                THEN '1'
            ELSE MAX(print__do_not_contact)
            END AS print__do_not_contact
        ,MAX(print__has_free_rate_code) AS print__has_free_rate_code
        ,MAX(print__round_id) AS print__round_id
        ,CASE 
            WHEN STRING_AGG(CAST('"' + print__product + '"' AS VARCHAR(MAX)), ',') IS NULL
                THEN '[]'
            ELSE '[' + STRING_AGG(CAST('"' + print__product + '"' AS VARCHAR(MAX)), ',') + ']'
            END AS print__product
        ,print__deceased
    INTO stage.matrix.hightouch_user_profiles_match_idm -- new table
        -- select count(*) from stage.matrix.hightouch_user_profiles_match_idm
    FROM sub_active
    GROUP BY primary_key
        ,marketing_id
        ,print__sub_id
        ,print__sub_type
        ,first_name
        ,last_name
        ,phone
        ,country
        ,email
        ,print__postcode
        ,print__deceased;
END;

GO