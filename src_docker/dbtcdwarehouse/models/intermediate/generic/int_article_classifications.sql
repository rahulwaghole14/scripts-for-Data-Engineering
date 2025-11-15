{{
    config(
        materialized = 'table',
        tags = ['ml'],
    )
}}

WITH articles AS (
    -- inspire me:
    SELECT 123 as article_id, 'Reading about the incredible journey of the mountaineers who scaled Everest fills me with a sense of adventure and determination.' AS text
    UNION ALL SELECT 456 as article_id, 'The story of the young entrepreneur who built a successful company from scratch is truly inspiring and motivational.'  AS text
    -- update me:
    UNION ALL SELECT 789 as article_id, 'The latest news update on the economic policies highlights the impact on the global markets.'  AS text
    UNION ALL SELECT 333 as article_id, 'Breaking news: A major breakthrough in medical research has been announced, promising new treatments for chronic diseases.'  AS text
    -- divert me:
    UNION ALL SELECT 444 as article_id, 'Enjoy this fun quiz to find out which character from your favorite TV show you are most like.'  AS text
    UNION ALL SELECT 555 as article_id, 'Get ready for an exciting weekend with these top movie recommendations that will keep you entertained.'  AS text

)

SELECT
    article_id,
    classification
FROM
    {{ classify_text_with_model(ref('remote_gemini'), '(SELECT article_id, text FROM articles)', 'text', 'article_id') }}
