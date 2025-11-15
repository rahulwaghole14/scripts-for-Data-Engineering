SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE OR ALTER PROCEDURE [idm].[load_braze_subs]
AS
BEGIN
    -- check if the table already exists
    IF OBJECT_ID('idm.stg_hightouch_sub_mapping', 'U') IS NOT NULL
    BEGIN
        -- if it does, drop it
        DROP TABLE idm.stg_hightouch_sub_mapping;
    END;

    -- create the table based on the results of the select statement
    ;WITH Subscriptions AS (
        SELECT
            marketing_id,
            'hexa-editorial-newsletter-' + LOWER(TRIM(JSON.acmNewsInternalName)) AS communication_reference,
            CASE
                WHEN user_consent = 'True' THEN 'subscribed'
                ELSE 'subscribed'
            END AS consent_status
        FROM
            [idm].[drupal__user_profiles]
        CROSS APPLY
            OPENJSON (newsletter_subs)
            WITH ( acmNewsInternalName NVARCHAR(100) '$.acmNewsInternalName' ) AS JSON
        WHERE newsletter_subs is not null
    )
    SELECT
        marketing_id,
        (
            SELECT communication_reference, consent_status
            FROM Subscriptions AS S2
            WHERE S2.marketing_id = S1.marketing_id
            FOR JSON PATH
        ) AS communication_subscriptions
    INTO idm.stg_hightouch_sub_mapping
    FROM Subscriptions AS S1
    GROUP BY marketing_id;

END;
GO
