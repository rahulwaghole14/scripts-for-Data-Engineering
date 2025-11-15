use stage;
-- hightouch model name
-- dev__cdw_to_braze__matrix_sub_status
CREATE OR ALTER VIEW matrix.hightouch_sub_mapping AS
with active_subscriptions as (
    select
    distinct subs_pointer
    , subscription.ProductID as product_id
    from stage.matrix.subscription
    left join stage.matrix.subord_cancel on subord_cancel.sord_pointer = subscription.sord_pointer
        and subord_cancel.ProductID = subscription.ProductID
    where ( subscription.sord_stopdate >= CAST(GETDATE() AS DATE) or subscription.sord_stopdate is null )
    and ( subord_cancel.canx_date >= CAST(GETDATE() AS DATE) or subord_cancel.canx_date is null )
    and ( subscription.exp_end_date >= CAST(GETDATE() AS DATE) or subscription.exp_end_date is null )
    group by subs_pointer, subscription.ProductID
)

 , subscribers as (
    SELECT lower(c.marketing_id) as marketing_id
        , c.subs_id as print__sub_id
        , s.subs_pointer as subs_pointer
    FROM [stage].[matrix].[customer] c
    inner join stage.matrix.subscriber s on s.subs_id = c.subs_id
    where marketing_id is not null
 )

 , mapping as (
    SELECT  'CHP' as product_id,'be06fe7f-06f4-4fa4-9c6e-6f676996ac25' as sub_group_id -- the_press_marketing 'CHP','CHPD'
    union all select 'CHPD' as product_id,'be06fe7f-06f4-4fa4-9c6e-6f676996ac25' as sub_group_id -- the_press_marketing 'CHP','CHPD'
    union all select 'CHP' as product_id,'e7389e62-6a75-4542-bec0-7f3ee16d82cb' as sub_group_id -- the_press_service 'CHP','CHPD'
    union all select 'CHPD' as product_id,'e7389e62-6a75-4542-bec0-7f3ee16d82cb' as sub_group_id -- the_press_service 'CHP','CHPD'
    union all select 'CHP' as product_id,'82082abd-d10a-4f7c-8caf-e1096ab59d1c' as sub_group_id -- the_press_subscriber_benefits 'CHP','CHPD'
    union all select 'CHPD' as product_id,'82082abd-d10a-4f7c-8caf-e1096ab59d1c' as sub_group_id -- the_press_subscriber_benefits 'CHP','CHPD'
)

 select
    r.marketing_id
    , m.sub_group_id
    , LOWER(CONVERT(NVARCHAR(MAX), HASHBYTES('SHA2_256', CONCAT(CAST(r.marketing_id AS NVARCHAR(MAX)), CAST(m.sub_group_id AS NVARCHAR(MAX)))), 2)) AS hash_key
 from active_subscriptions s
 left join subscribers r on r.subs_pointer = s.subs_pointer
 left join mapping m on m.product_id = s.product_id
 where r.marketing_id is not null
 and m.sub_group_id is not null
