SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [mather].[load_vault] AS

	-- Usage
	-- exec [stage].[PRESSPATRON].[load_vault];

insert into [stage].[log].[etl_job] ([user],[source],[job],[status]) values (CURRENT_USER,OBJECT_SCHEMA_NAME(@@PROCID),OBJECT_NAME(@@PROCID),'started');

/*
	delete from [vault].[raw].[link_customer_retention] where record_source = 'MATHER';
	delete from [vault].[raw].[sat_customer_retention] where record_source = 'MATHER';
*/

/* */

	-- truncate table [vault].[raw].[link_customer_retention];
	insert into [vault].[raw].[link_customer_retention] (
		 customer_retention_hash
		,customer_hash
		,product_hash

		,dmh.record_source
		,record_load_dts
	)
	select distinct
		 dmh.hash_key
		,customer_hash
		,product_hash
		-- this is not unique. remove? #todo
--		,cast(dmh.filedate as varchar) + cast(dmh.sub_id as varchar) + pub as cust_retention_id

		,dmh.record_source
		,dmh.record_load_dts

  FROM [stage].[delta].[masthead_hist] dmh
  inner join (select distinct customer_hash,customer_id from vault.raw.sat_customer where record_source = 'MATRIX' and record_load_end_dts is null) cust
  on (sub_id = customer_id)
  inner join (select distinct product_hash,[description] from vault.raw.sat_product where record_source = 'MATRIX' and record_load_end_dts is null) prod
  on (pub = [description])
  where not exists (select customer_retention_hash from [vault].[raw].[link_customer_retention] where link_customer_retention.customer_retention_hash = dmh.hash_key and link_customer_retention.record_source = 'MATHER')
  -- and filedate > '2020-06-20'
	;

--	truncate table [vault].[raw].[sat_customer_retention]
	insert into [vault].[raw].[sat_customer_retention] (
				 customer_retention_hash
				,customer_retention_id
				,[received_dt]
				,new_rate
				,CLV
				,Churn_score
				,record_source
				,hash_diff
			   )
	select distinct
		 dmh.hash_key
		,cast(dmh.filedate as varchar) + cast(dmh.sub_id as varchar)+replace(lower(pub),' ','') as cust_retention_id
		,dmh.filedate
		,dmh.new_rate
		,dmh.CLV
		,dmh.churn_score
		,dmh.record_source--dmh.record_source
		,dmh.hash_diff
	-- select count(1),count(distinct matrix_subscription.hash_key)
	from [stage].[delta].[masthead_hist] dmh
	left outer join [vault].[raw].[sat_customer_retention] ON (
			customer_retention_hash = dmh.hash_key
		and sat_customer_retention.record_load_end_dts IS NULL
		and sat_customer_retention.record_source = 'MATHER')
	where dmh.hash_diff != coalesce(sat_customer_retention.hash_diff, '')
--	and filedate > '2020-06-20'	;
	-- select top 1000 * from [vault].[raw].[sat_subscription] where record_source = 'MATRIX';

	update [vault].[raw].[sat_customer_retention]
	set record_load_end_dts = sat_max.record_load_dts
	from [vault].[raw].[sat_customer_retention] sat
	inner join
	(
		select customer_retention_hash,max(record_load_dts) as record_load_dts
		from [vault].[raw].[sat_customer_retention]
		where record_source = 'MATHER'
	group by customer_retention_hash
	) as sat_max
	on sat.customer_retention_hash = sat_max.customer_retention_hash
	-- and sat.record_load_end_dts is null
	and sat.record_load_dts < sat_max.record_load_dts
	and record_source = 'MATHER'
	;

insert into [stage].[log].[etl_job] ([user],[source],[job],[status]) values (CURRENT_USER,OBJECT_SCHEMA_NAME(@@PROCID),OBJECT_NAME(@@PROCID),'finished');
GO
