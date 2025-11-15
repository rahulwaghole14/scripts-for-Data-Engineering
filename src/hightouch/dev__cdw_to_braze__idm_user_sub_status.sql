select
    lower(marketing_id) as marketing_id
    , communication_subscriptions
from stage.idm.stg_hightouch_sub_mapping
