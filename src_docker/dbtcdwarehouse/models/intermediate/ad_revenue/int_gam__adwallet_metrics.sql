{{
  config(
        tags=['adwallet']
    )
}}

WITH adwallet_data AS (
    SELECT
        creative.id AS creative_id,
        lineItem.id AS lineitem_id,
        `order`.id AS order_id,
        device AS device,
        ARRAY_AGG(DISTINCT screenshotURL IGNORE NULLS) AS screenshot_urls,
        ARRAY_AGG(DISTINCT extraAssets.thumbnail320 IGNORE NULLS) AS thumbnail320s,
        ARRAY_AGG(DISTINCT takenAtURL IGNORE NULLS) AS taken_at_urls
    FROM {{ ref('base_googleadmanager_adwallet_data') }}
    GROUP BY 1, 2, 3, 4
)

, pca_view_video_adwallet AS (
    SELECT
        *
    FROM {{ ref('base_googleadmanager_pca_view_video_adwallet') }}
)

SELECT pca_view_video_adwallet.*
 , adwallet_data.screenshot_urls as ADWALLET_SCREENSHOT_URLS
 , adwallet_data.thumbnail320s as ADWALLET_THUMBNAIL320S
 , adwallet_data.taken_at_urls as ADWALLET_TAKEN_AT_URLS
FROM pca_view_video_adwallet
LEFT JOIN adwallet_data ON ( pca_view_video_adwallet.CREATIVE_ID = adwallet_data.creative_id )
AND ( pca_view_video_adwallet.LINE_ITEM_ID = adwallet_data.lineitem_id )
AND ( pca_view_video_adwallet.ORDER_ID = adwallet_data.order_id )
AND ( lower(trim(pca_view_video_adwallet.DEVICE_CATEGORY_NAME)) = lower(trim(adwallet_data.device)) )
