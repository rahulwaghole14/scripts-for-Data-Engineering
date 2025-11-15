select lower(marketing_id) as marketing_id
  , 'b1b5e580-743c-4067-ad67-0d5da82d5e46' as sub_group_id -- general marketing hexa.co.nz_marketing
  from stage.idm.stg_hightouch_sub_mapping
