use stage;
exec stage.idm.load_braze_subs;
-- hightouch model name
-- dev__cdw_to_braze__matrix_sub_status


-- *****************
-- hightouch
-- select marketing_id
-- , print_communication_subscriptions
-- from stage.matrix.hightouch_print_communication_subscriptions
-- ;
;

---------------------------------- new -----
use stage;

CREATE OR ALTER VIEW matrix.hightouch_print_communication_subscriptions AS

with data_tables as (
    select * from stage.acm.generic_magazine_print_subs
    union all
    select * from stage.acm.generic_print_subs
)

, grouped as (
    select distinct marketing_id, internal_name from data_tables
)

, all_subs as (
        select customer.marketing_id from stage.matrix.customer
)

SELECT
    marketing_id,
    '[' + STRING_AGG(
        '{"communication_reference":"' + internal_name + '","consent_status":"subscribed"}', ','
    ) + ']' AS print_communication_subscriptions

FROM grouped
GROUP BY marketing_id

union all

select distinct marketing_id, '[]' as print_communication_subscriptions
from all_subs
where marketing_id not in ( select marketing_id from grouped )

;
