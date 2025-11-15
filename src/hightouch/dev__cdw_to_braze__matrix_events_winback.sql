-- event: print_winback
    -- ## winback event ##
    -- # LAST 9 WEEKS #
    -- print_winback
    -- print_winback_proposed_rate
    -- print_winback_product_id
    -- print_winback_rate_id
    -- print_winback_cancellation_date
    -- print__winback_cancelled_pubs
    -- print__winback_cancelled_proposed_rate
    -- print__cancelled_multiple_pubs -- {{ row['print__cancelled_multiple_pubs'] | cast : 'boolean' }}

-- hightouch name: {dev|prod}__cdw_to_braze__matrix_events_winback

select
    lower(c.marketing_id) as marketing_id,
    sub2.Proposed_Rate as print__winback_proposed_rate,
    sub2.ProductID as print__winback_product_id,
    sub2.RateID as print__winback_rate_id,
    FORMAT(CAST( sub2.Cancellation_Date AS DATETIME), 'yyyy-MM-dd') as print__winback_cancellation_date,
    sub2.Week_No as print__winback_week,
    sub2.Cancelled_Pubs as print__winback_cancelled_pubs,
    sub2.Cancelled_Proposed_Rate as print__winback_cancelled_proposed_rate,
    sub2.Cancelled_Multiple_Pubs as print__cancelled_multiple_pubs,
    sub2.Cancellation_ID as print__winback_cancellation_id,
    CURRENT_TIMESTAMP as timestamp_dts
    from (
select
    Sub_Id as print__sub_id,
    CASE
        WHEN STRING_AGG(CAST(Proposed_Rate AS VARCHAR(MAX)), ',') IS NULL
        THEN '[]'
        ELSE '[' + STRING_AGG('"' + CAST(Proposed_Rate AS VARCHAR(MAX)) + '"', ',') + ']'
    END as Proposed_Rate,
    CASE
        WHEN STRING_AGG(trim(ProductID), ',') IS NULL
        THEN '[]'
        ELSE '[' + STRING_AGG('"' + trim(ProductID) + '"', ',') + ']'
    END as ProductID,
    CASE
        WHEN STRING_AGG(trim(RateID), ',') IS NULL
        THEN '[]'
        ELSE '[' + STRING_AGG('"' + trim(RateID) + '"', ',') + ']'
    END as RateID,
    CASE
        WHEN STRING_AGG(trim(Week_No), ',') IS NULL
        THEN '[]'
        ELSE '[' + STRING_AGG('"' + trim(Week_No) + '"', ',') + ']'
    END as Week_No,
    CASE
        WHEN STRING_AGG(trim(Cancelled_Pubs), ',') IS NULL
        THEN '[]'
        ELSE '[' + STRING_AGG('"' + trim(Cancelled_Pubs) + '"', ',') + ']'
    END as Cancelled_Pubs,
    CASE
        WHEN STRING_AGG(trim(Cancelled_Proposed_Rate), ',') IS NULL
        THEN '[]'
        ELSE '[' + STRING_AGG('"' + trim(Cancelled_Proposed_Rate) + '"', ',') + ']'
    END as Cancelled_Proposed_Rate,
    CASE
        WHEN STRING_AGG(trim(Cancellation_ID), ',') IS NULL
        THEN '[]'
        ELSE '[' + STRING_AGG('"' + trim(Cancellation_ID) + '"', ',') + ']'
    END as Cancellation_ID,
    case when max(Cancelled_Multiple_Pubs) = 'YES' THEN 1 ELSE 0 END as Cancelled_Multiple_Pubs,
    max(Cancellation_Date) as Cancellation_Date
from
(
    select
        Sub_Id,
        Proposed_Rate,
        ProductID,
        RateID,
        Cancellation_Date,
        Week_No,
        Cancelled_Pubs,
        Cancelled_Proposed_Rate,
        Cancelled_Multiple_Pubs,
        Cancellation_ID
    from
        stage.matrix.temp_winback_subscribers1
) sub
group by
    Sub_Id ) sub2
    left join
    [stage].[matrix].[customer] c
on
    c.subs_id = sub2.print__sub_id
