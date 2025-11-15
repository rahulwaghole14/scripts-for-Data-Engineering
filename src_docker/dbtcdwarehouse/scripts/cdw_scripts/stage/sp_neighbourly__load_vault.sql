SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE   PROCEDURE [neighbourly].[load_vault] AS

	-- Usage
	-- exec [stage].[neighbourly].[load_vault];

	insert into [stage].[log].[etl_job] ([user],[source],[job],[status]) values (CURRENT_USER,OBJECT_SCHEMA_NAME(@@PROCID),OBJECT_NAME(@@PROCID),'started');

	/*
	delete from [vault].[raw].[sat_customer] where record_source = 'neighbourly';
	delete from [vault].[raw].[hub_customer] where record_source = 'neighbourly';
	*/

	insert into [vault].[raw].[hub_customer] (customer_hash,customer_id,record_source,record_load_dts)
	select distinct
		   hash_key
		  ,id
		  ,record_source
		  ,record_load_dts
	from [stage].[neighbourly].[user]
	where not exists (select customer_hash from [vault].[raw].[hub_customer] where hub_customer.customer_hash = [user].hash_key and hub_customer.record_source = 'neighbourly')
	;


	-- loading new customer record into scv delta
	exec [delta].[load_scv_import] 'neighbourly'
	;



	insert into [vault].[raw].[sat_customer] (
		 customer_hash
		,customer_id
		--,[type]
		,active
		--,[status]
		,first_name
		-- ,middle_name
		,last_name
		--,dob
		,email --,email_alternative
		--,phone_home
		--,phone_work
		,phone_mobile
		,address_type
		,address_1,address_2,address_3,address_4
		-- ,dpid
		--,latitude,longitude
		--,country
		,record_source,record_load_dts,hash_diff
	)

	select distinct
		 [user].hash_key
		,[user].id
		,[user].is_active
		,nullif([user].first_name,'')
		,nullif([user].last_name,'')
		,substring([user].email,0,128)
		,[user].mobile

		,[paf_address].address_type
		,nullif(trim(trim(trim([paf_address].[unit_type] + ' ' + [paf_address].[unit_identifier]) + ' ' + [paf_address].[floor]) + ' ' + [paf_address].[building_name]),'') as address_1
		,nullif(trim(trim(cast([paf_address].[street_number] as varchar) + [paf_address].[street_alpha] + ' ' + [paf_address].[street_name]) + ' ' + [paf_address].[street_type]),'') as address_2
		,nullif([suburb_name],'') as address_3
		,nullif(trim([town_city_mailtown] + ' ' + [postcode]),'') as address_4

		,[user].record_source,[user].record_load_dts
		,[user].hash_diff

		-- select count(1),count(distinct [user].id)
		-- select top 1000 *
	from [stage].[neighbourly].[user]
	left join (select user_id,max(id) id from [stage].[neighbourly].[user_paf_address] group by user_id ) as max_paf on [user].id = max_paf.user_id
	left join  [stage].[neighbourly].[user_paf_address] on max_paf.id = [user_paf_address].id
	left join [stage].[neighbourly].[paf_address] on [paf_address].id = [user_paf_address].[paf_address_id]

	left outer join [vault].[raw].[sat_customer] ON (
					sat_customer.customer_hash = [user].hash_key
				and sat_customer.record_load_end_dts IS NULL
				and sat_customer.record_source = 'neighbourly'
				)
	where	[user].hash_diff != coalesce(sat_customer.hash_diff, '')
		and [user_paf_address].moved_out_at is null
	;
	-- select top 100 * from [vault].[raw].[sat_customer] where record_source = 'neighbourly';
	-- select count(1),count(distinct customer_hash) from [vault].[raw].[sat_customer] where record_source = 'neighbourly';

	update [vault].[raw].[sat_customer]
	set record_load_end_dts = sat_max.record_load_dts
	from [vault].[raw].[sat_customer] sat
	inner join
	(
		select customer_hash,max(record_load_dts) as record_load_dts
		from [vault].[raw].[sat_customer]
		where record_source = 'neighbourly'
		group by customer_hash
	) as sat_max
	on sat.customer_hash = sat_max.customer_hash
	and sat.record_load_end_dts is null
	and sat.record_load_dts < sat_max.record_load_dts
	and sat.record_source = 'neighbourly'
	;


	insert into [stage].[log].[etl_job] ([user],[source],[job],[status]) values (CURRENT_USER,OBJECT_SCHEMA_NAME(@@PROCID),OBJECT_NAME(@@PROCID),'finished');
GO
