-- event: print_complaint
-- # LAST 9 WEEKS #
    -- print_complaint (timestamp of the event when its pushed in)
    -- print_complaint_product -- not sure if this should be custom attr because it is event below
    -- print_complaint_type -- not sure if this should be custom attr because it is event below
    -- print_complaint_date

-- hightouch:
-- model/sync name dev__cdw_to_braze__matrix_events_complaints

-- event name: print_complaints

-- select
--   primary_key
--   , marketing_id
--   , print_complaint_product -- {{ row['print_complaint_product'] | parse }}
--   , print_complaint_type -- {{ row['print_complaint_type'] | parse }}
--   , print_complaint_date -- {{ row['print_complaint_date'] | to_date }}
-- from stage.matrix.hightouch_events_complaints
-- where primary_key is not null;


CREATE OR ALTER VIEW matrix.hightouch_events_complaints AS
WITH subscribers AS (

    SELECT c.hash_key AS primary_key,
        LOWER(c.marketing_id) AS marketing_id,
        c.subs_id AS print__sub_id
    FROM [stage].[matrix].[customer] c

)

, complaints AS (
    SELECT [sord_pointer]
      , [comp_date] as print_complaint_date
      , [ProductID] as print_complaint_product
      , [comp_type] as print_complaint_type
    FROM [stage].[matrix].[complaints]
    -- yesterday?
    WHERE CAST(comp_date AS DATE) = CAST(DATEADD(DAY, -1, SWITCHOFFSET(SYSDATETIMEOFFSET(), '+12:00')) AS DATE)
    -- 9 weeks
    -- WHERE [comp_date] >= DATEADD(week, -9, GETDATE())
)

, subs as (
    SELECT
        sord_pointer
        , subscriber.subs_id
    FROM stage.matrix.subscription
    LEFT JOIN stage.matrix.subscriber ON subscriber.subs_pointer = subscription.subs_pointer
)

SELECT
  subs_id -- ignore hightouch
  , max(primary_key) as primary_key
  , max(marketing_id) as marketing_id
  , '[' + STRING_AGG('"' + CAST(TRIM(complaints.print_complaint_product) AS NVARCHAR(MAX)) + '"', ', ') + ']' AS print_complaint_product
  , '[' + STRING_AGG('"' + CAST(TRIM(complaints.print_complaint_type) AS NVARCHAR(MAX)) + '"', ', ') + ']' AS print_complaint_type
  , max(print_complaint_date) as print_complaint_date
FROM complaints
left join subs on subs.sord_pointer = complaints.sord_pointer
left join subscribers on subscribers.print__sub_id = subs.subs_id
group by subs.subs_id
