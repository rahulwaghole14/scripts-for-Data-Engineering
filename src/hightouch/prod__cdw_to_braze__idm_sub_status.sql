select lower(marketing_id) as marketing_id
  , 'acaa104a-65ad-47e1-84e7-3ed6359cce60' as sub_group_id -- general marketing hexa.co.nz_marketing
  from stage.idm.stg_hightouch_sub_mapping
