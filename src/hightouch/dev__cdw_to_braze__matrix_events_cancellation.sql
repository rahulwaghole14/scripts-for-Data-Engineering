-- use smart;
-- exec raw.load_smart;

-- event: print_cancellation
    -- # events # LAST 9 WEEKS
    -- print_cancellation (event) Required to know when to begin Winback  (timestamp of the event when its pushed in)
    -- print_cancellation_code (Event property of above)
    -- print_cancellation_product_id  (event property of above)
    -- print_subscription_term/period (event property of above)
    -- print_cancellation_date

-- use stage;

-- hightouch:
-- model/sync name dev__cdw_to_braze__matrix_events_cancellation

-- event name: print_cancellation

-- select
--   primary_key
--   , marketing_id
--   , print_subscription_period -- {{ row['print_subscription_period'] | parse }}
--   , print_cancellation_product_id -- {{ row['print_cancellation_product_id'] | parse }}
--   , print_cancellation_code -- {{ row['print_cancellation_code'] | parse }}
--   , print_cancellation_date -- {{ row['print_cancellation_date'] | to_date }}
-- from stage.matrix.hightouch_events_cancellation
-- where primary_key is not null


CREATE OR ALTER VIEW matrix.hightouch_events_cancellation AS
WITH subscribers AS (

    SELECT c.hash_key AS primary_key,
        LOWER(c.marketing_id) AS marketing_id,
        c.subs_id AS print__sub_id
    FROM [stage].[matrix].[customer] c

),

active_subs AS (

    SELECT
        -- subs_pointer,
        subscriber.subs_id as print__sub_id,
        '[' + STRING_AGG('"' + CAST(subscription.period_id AS NVARCHAR(MAX)) + '"', ', ') + ']' AS print_subscription_period,
        '[' + STRING_AGG('"' + CAST(subord_cancel.ProductID AS NVARCHAR(MAX)) + '"', ', ') + ']' AS print_cancellation_product_id,
        '[' + STRING_AGG('"' + CAST(subord_cancel.canx_id AS NVARCHAR(MAX)) + '"', ', ') + ']' AS print_cancellation_code,
        max(subord_cancel.canx_date) AS print_cancellation_date
    FROM stage.matrix.subscription
    LEFT JOIN stage.matrix.subord_cancel
        ON subord_cancel.sord_pointer = subscription.sord_pointer
        AND subord_cancel.ProductID = subscription.ProductID
    LEFT JOIN stage.matrix.subscriber ON subscriber.subs_pointer = subscription.subs_pointer
    WHERE
        (subscription.sord_stopdate < CAST(SWITCHOFFSET(SYSDATETIMEOFFSET(), '+12:00') AS DATE)
            OR subscription.sord_stopdate IS NOT NULL)
        AND (subord_cancel.canx_date < CAST(SWITCHOFFSET(SYSDATETIMEOFFSET(), '+12:00') AS DATE)
            OR subord_cancel.canx_date IS NOT NULL)
        AND (subscription.exp_end_date < CAST(SWITCHOFFSET(SYSDATETIMEOFFSET(), '+12:00') AS DATE)
            OR subscription.exp_end_date IS NOT NULL)
        -- yesterday
        AND CAST(subord_cancel.canx_date AS DATE) = CAST(DATEADD(DAY, -1, SWITCHOFFSET(SYSDATETIMEOFFSET(), '+12:00')) AS DATE)
        -- 9 weeks
        -- AND subord_cancel.canx_date >= DATEADD(week, -9, GETDATE())  -- Last 9 weeks
    GROUP BY subscriber.subs_id

)

  SELECT
    subscribers.primary_key as primary_key
    , subscribers.marketing_id as marketing_id
    , active_subs.*
  FROM active_subs
  left join subscribers on subscribers.print__sub_id = active_subs.print__sub_id
