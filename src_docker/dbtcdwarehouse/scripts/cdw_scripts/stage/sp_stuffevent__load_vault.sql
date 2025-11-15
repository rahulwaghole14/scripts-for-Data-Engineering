SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [hexaevent].[load_vault] AS

	-- Usage
	-- exec [stage].[hexaevent].[load_vault];

	insert into [stage].[log].[etl_job] ([user],[source],[job],[status]) values (CURRENT_USER,OBJECT_SCHEMA_NAME(@@PROCID),OBJECT_NAME(@@PROCID),'started');

/*
	delete from [vault].[raw].[sat_customer] where record_source = 'hexaEVENT';
	delete from [vault].[raw].[hub_customer] where record_source = 'hexaEVENT';
*/

	insert into [vault].[raw].[hub_customer] (customer_hash,customer_id,record_source,record_load_dts)
	select distinct
		   hash_key
		  ,cast(abs([customer_id]) as bigint) as [customer_id]
		  ,record_source
		  ,record_load_dts
	from [stage].[hexaEVENT].[cust_detail]
	where not exists (select customer_hash from [vault].[raw].[hub_customer] where hub_customer.customer_hash = [cust_detail].hash_key and hub_customer.record_source = 'hexaEVENT')
	;

	-- loading new customer record into scv delta
	exec [delta].[load_scv_import] 'hexaEVENT'
	;


	insert into [vault].[raw].[sat_customer] (
		 customer_hash
		,customer_id
		,[type]
		--,active
		,[status] -- when the customer join an event
		,first_name
		-- ,middle_name
		,last_name
		--,dob
		,email --,email_alternative
		,phone_home
		--,phone_work
		,phone_mobile
		--,address_type
		,address_1
		,address_2,address_3,address_4
		-- ,dpid
		--,latitude,longitude
		--,country
		,record_source,record_load_dts,hash_diff
	)

	select distinct
		 [cust_detail].hash_key
		,cast(abs([cust_detail].[customer_id]) as bigint) [customer_id]
		,[cust_detail].[type]
		,[cust_detail].[transaction date]
		,[cust_detail].[first name]
		,[cust_detail].[last name]
		,[cust_detail].email
		,[cust_detail].telephone
		,[cust_detail].mobile
		,[cust_detail].[address] as address_1
		,[cust_detail].suburb as address_2
		,[cust_detail].city as address_3
		,[cust_detail].postcode as address_4

		,[cust_detail].record_source,[cust_detail].record_load_dts
		,[cust_detail].hash_diff

		-- select count(1),count(distinct [cust_detail].[generated_cust_id])
		-- select top 1000 *
	from [stage].[hexaEVENT].[cust_detail]
	left outer join [vault].[raw].[sat_customer] ON (
					sat_customer.customer_hash = [cust_detail].hash_key
				and sat_customer.record_load_end_dts IS NULL
				and sat_customer.record_source = 'hexaEVENT'
				)
	where	[cust_detail].hash_diff != coalesce(sat_customer.hash_diff, '')
	;
	-- select top 100 * from [vault].[raw].[sat_customer] where record_source = 'hexaEVENT';
	-- select count(1),count(distinct customer_hash) from [vault].[raw].[sat_customer] where record_source = 'hexaEVENT';

	update [vault].[raw].[sat_customer]
	set record_load_end_dts = sat_max.record_load_dts
	from [vault].[raw].[sat_customer] sat
	inner join
	(
		select customer_hash,max(record_load_dts) as record_load_dts
		from [vault].[raw].[sat_customer]
		where record_source = 'hexaEVENT'
		group by customer_hash
	) as sat_max
	on sat.customer_hash = sat_max.customer_hash
	and sat.record_load_end_dts is null
	and sat.record_load_dts < sat_max.record_load_dts
	and sat.record_source = 'hexaEVENT'
	;


	insert into [stage].[log].[etl_job] ([user],[source],[job],[status]) values (CURRENT_USER,OBJECT_SCHEMA_NAME(@@PROCID),OBJECT_NAME(@@PROCID),'finished');
GO
