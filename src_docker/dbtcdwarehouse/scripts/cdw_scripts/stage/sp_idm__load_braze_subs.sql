
-- USE [stage];
-- GO

-- IF OBJECT_ID ('idm.load_braze_subs', 'P') IS NOT NULL
--     DROP PROCEDURE idm.load_braze_subs;
-- GO

CREATE OR ALTER PROCEDURE idm.load_braze_subs
AS
BEGIN
    -- Preparing the source data
    ;WITH Subscriptions AS (
        SELECT
            marketing_id,
            'hexa-editorial-newsletter-' + LOWER(JSON.acmNewsInternalName) AS communication_reference,
            CASE
                WHEN user_consent = 'True' THEN 'subscribed'
                ELSE 'unsubscribed'
            END AS consent_status
        FROM
            [idm].[drupal__user_profiles]
        CROSS APPLY
            OPENJSON (newsletter_subs)
            WITH ( acmNewsInternalName NVARCHAR(100) '$.acmNewsInternalName' ) AS JSON
        WHERE newsletter_subs is not null
    ),
    SourceData AS (
        SELECT
            marketing_id,
            (
                SELECT communication_reference, consent_status
                FROM Subscriptions AS S2
                WHERE S2.marketing_id = S1.marketing_id
                FOR JSON PATH
            ) AS communication_subscriptions
        FROM Subscriptions AS S1
        GROUP BY marketing_id
    )
    -- Merge statement
    MERGE idm.stg_hightouch_sub_mapping AS Target
    USING SourceData AS Source
    ON (Target.marketing_id = Source.marketing_id)
    -- When matched, then update
    WHEN MATCHED THEN
        UPDATE SET Target.communication_subscriptions = Source.communication_subscriptions
    -- When NOT matched by source (record exists in target but not in source), then delete
    WHEN NOT MATCHED BY SOURCE THEN
        DELETE
    -- When NOT matched by target (record exists in source but not in target), then insert
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (marketing_id, communication_subscriptions)
        VALUES (Source.marketing_id, Source.communication_subscriptions);

    -- Rebuild the index
    ALTER INDEX idx_marketing_id ON idm.stg_hightouch_sub_mapping REBUILD;
END;
GO
