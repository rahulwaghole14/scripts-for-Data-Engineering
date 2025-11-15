-- event: print_purchase

--     #####################
--     ## purchase events ##
--     # LAST 9 WEEKS #
--     print_purchase (timestamp of the event when its pushed in)
--     print_product_purchased <-- property of above
--     print_subscription_date
--     print_subscription_period


-- hightouch model name: dev__cdw_to_braze__matrix_events_purchase

-- hightouch query:

-- select primary_key
-- , lower(marketing_id) as marketing_id
-- , print_subscription_period -- {{ row['print_subscription_period'] | parse }}
-- , print_product_purchased -- {{ row['print_product_purchased'] | parse }}
-- , print_subscription_date -- {{ row['print_subscription_date'] | to_date }}
-- from stage.matrix.hightouch_events_purchase

CREATE OR ALTER VIEW matrix.hightouch_events_purchase AS
WITH subscribers AS (

    SELECT c.hash_key AS primary_key,
        LOWER(c.marketing_id) AS marketing_id,
        c.subs_id AS print__sub_id
    FROM [stage].[matrix].[customer] c

)

, purchased_subs as (
    SELECT
    -- top(200) -- limit for dev
      -- subs_pointer,
      subscriber.subs_id as print__sub_id
      , '[' + STRING_AGG('"' + CAST(subscription.period_id AS NVARCHAR(MAX)) + '"', ', ') + ']' AS print_subscription_period
      , '[' + STRING_AGG('"' + CAST( subscription.productid AS NVARCHAR(MAX)) + '"', ', ') + ']' as print_product_purchased
      , max(subscription.sord_startdate) as print_subscription_date
    FROM stage.matrix.subscription
    LEFT JOIN stage.matrix.subord_cancel
        ON subord_cancel.sord_pointer = subscription.sord_pointer
        AND subord_cancel.ProductID = subscription.ProductID
    LEFT JOIN stage.matrix.subscriber ON subscriber.subs_pointer = subscription.subs_pointer
    -- not cancelled
    where ( subscription.sord_stopdate >= CAST(GETDATE() AS DATE) or subscription.sord_stopdate is null )
    and ( subord_cancel.canx_date >= CAST(GETDATE() AS DATE) or subord_cancel.canx_date is null )
    and ( subscription.exp_end_date >= CAST(GETDATE() AS DATE) or subscription.exp_end_date is null )
    -- yesterday only
    AND subscription.sord_startdate < CAST(SWITCHOFFSET(SYSDATETIMEOFFSET(), '+12:00') AS DATE)
    AND CAST(subscription.sord_startdate AS DATE) = CAST(DATEADD(DAY, -1, SWITCHOFFSET(SYSDATETIMEOFFSET(), '+12:00')) AS DATE)
    -- last 9 weeks
    -- AND subscription.sord_startdate >= DATEADD(week, -9, GETDATE())
    GROUP BY subscriber.subs_id
)

  SELECT
    subscribers.primary_key as primary_key
    , subscribers.marketing_id as marketing_id
    , purchased_subs.*
  FROM purchased_subs
  left join subscribers on subscribers.print__sub_id = purchased_subs.print__sub_id
