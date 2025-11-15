
````sql
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE   PROCEDURE [matrix].[load_vault] AS

	-- Usage
	-- exec [stage].[matrix].[load_vault];

	insert into [stage].[log].[etl_job] ([user],[source],[job],[status]) values (CURRENT_USER,OBJECT_SCHEMA_NAME(@@PROCID),OBJECT_NAME(@@PROCID),'started');

	-- POST STAGE

	alter index all on [stage].[matrix].[customer] disable;

	truncate table [stage].[matrix].[customer];
	-- select count(1) from [stage].[matrix].[customer];
	INSERT INTO [stage].[matrix].[customer]
	(
	hash_key,subs_id,subs_type,title,FirstName,SurName,PersContact3
	,HOME,WORK,MOBILE,DoNotCall
	,AddrLine3,AddrLine4,AddrLine5,AddrLine6,CountryName,
	record_source,record_load_dts,hash_diff

	)
	select distinct
		 [subscriber].hash_key
		,[subscriber].subs_id
		,[subscriber].subtype_id
		,[tbl_person].title,[tbl_person].FirstName,[tbl_person].SurName,[tbl_person].PersContact3
		,contact.HOME, contact.WORK, contact.MOBILE,contact.DoNotCall
		,[tbl_location].AddrLine3
		,[tbl_location].AddrLine4
		,[tbl_location].AddrLine5
		,[tbl_location].AddrLine6
		,[tbl_country].CountryName

		,[subscriber].record_source
		,[subscriber].record_load_dts
		,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' as hash_diff


	from [stage].[matrix].[subscriber]
	left join [stage].[matrix].[tbl_person]  		on [tbl_person].person_pointer = [subscriber].subs_perid
	left join [stage].[matrix].[tbl_location]		on [tbl_location].Level1Pointer = [tbl_person].ObjectPointer
	left join [stage].[matrix].[tbl_country]		on [tbl_location].Country_Code = [tbl_country].Country_Code
	left join (
		select b.ObjectPointer,HOME,WORK,MOBILE,max(c.donotcall) as DoNotCall
		from (
			select ObjectPointer,ContactType, max(concat(AreaCode,ContactNumber)) phone
			from [stage].[matrix].[tbl_contact_number]
			--where ObjectPointer = 4261361
			group by ObjectPointer,ContactType
		) a pivot (max(phone) for contacttype in (HOME,WORK,MOBILE) ) as b
		inner join [stage].[matrix].[tbl_contact_number] c on b.ObjectPointer = c.ObjectPointer
		group by b.ObjectPointer,HOME,WORK,MOBILE
	) contact on [tbl_person].ObjectPointer = [contact].ObjectPointer
	;

	update [stage].[matrix].[customer]
	set hash_diff =
		substring(
			convert(char,hashbytes('MD5',
				concat_ws('|',coalesce(title,''),coalesce(FirstName,''),coalesce(SurName,''),coalesce(PersContact3,''),coalesce(home,''), coalesce(work,''),coalesce(mobile,''),coalesce(AddrLine3,''),coalesce(AddrLine4,''),coalesce(AddrLine5,''),coalesce(AddrLine6,''),coalesce(CountryName,''))
			),2)
		,3,32)
	;

	alter index all on [stage].[matrix].[customer] rebuild;


	------------------------------------------------------------------------------------------------
	-- Load [vault] from [stage]

	/*
	delete from [vault].[raw].[sat_customer]			where record_source = 'MATRIX';
	delete from [vault].[raw].[hub_customer]			where record_source = 'MATRIX';
	delete from [vault].[raw].[sat_service]				where record_source = 'MATRIX';
	delete from [vault].[raw].[hub_service]				where record_source = 'MATRIX';
	delete from [vault].[raw].[sat_product]				where record_source = 'MATRIX';
	delete from [vault].[raw].[hub_product]				where record_source = 'MATRIX';
	delete from [vault].[raw].[sat_cancellation_reason]	where record_source = 'MATRIX';
	delete from [vault].[raw].[hub_cancellation_reason]	where record_source = 'MATRIX';
	delete from [vault].[raw].[link_subscription] 		where record_source = 'MATRIX';
	delete from [vault].[raw].[sat_subscription] 		where record_source = 'MATRIX';
	delete from [vault].[raw].[link_cancellation] 		where record_source = 'MATRIX';
	delete from [vault].[raw].[sat_cancellation] 		where record_source = 'MATRIX';
	*/
	-- Currently takes 2:30 minutes

	insert into [vault].[raw].[hub_customer] (customer_hash,customer_id,record_source,record_load_dts)
	select distinct
		   hash_key
		  ,subs_id
		  ,record_source
		  ,record_load_dts
	from [stage].[matrix].[customer]
	where not exists (select customer_hash from [vault].[raw].[hub_customer] where hub_customer.customer_hash = customer.hash_key and hub_customer.record_source = 'MATRIX')
	;


	-- loading new customer record into scv delta
	exec [delta].[load_scv_import] 'matrix'
	;



	insert into [vault].[raw].[sat_customer] (
		 customer_hash
		,customer_id
		,[type]
		,title
		,first_name
		-- ,middle_name
		,last_name
		--,dob
		,email
		-- ,email_alternative
		,phone_home,phone_work,phone_mobile
		,address_1,address_2,address_3,address_4
		-- ,dpid
		,country
		,record_source
		,record_load_dts
		,hash_diff
	)
	select distinct
		   customer.hash_key
		  ,customer.subs_id
		  ,customer.subs_type
		  ,customer.title,customer.FirstName,customer.SurName,customer.PersContact3
		  ,customer.HOME,customer.WORK,customer.MOBILE
		  ,customer.AddrLine3
		  ,customer.AddrLine4
		  ,customer.AddrLine5
		  ,customer.AddrLine6
		  ,customer.CountryName

		  ,customer.record_source
		  ,customer.record_load_dts
		  ,customer.hash_diff

	from [stage].[matrix].[customer]
		left outer join [vault].[raw].[sat_customer] ON (
						sat_customer.customer_hash = customer.hash_key
					and sat_customer.record_load_end_dts IS NULL
					and sat_customer.record_source = 'MATRIX'
					)
	where	customer.hash_diff != coalesce(sat_customer.hash_diff, '')
	;

	update [vault].[raw].[sat_customer]
	set record_load_end_dts = sat_max.record_load_dts
	from [vault].[raw].[sat_customer] sat
	inner join
	(
		select customer_hash,max(record_load_dts) as record_load_dts
		from [vault].[raw].[sat_customer]
		where record_source = 'MATRIX'
		group by customer_hash
	) as sat_max
	on sat.customer_hash = sat_max.customer_hash
	and sat.record_load_end_dts is null
	and sat.record_load_dts < sat_max.record_load_dts
	and sat.record_source = 'MATRIX'
	;

	insert into [vault].[raw].[hub_product] (product_hash,product_id,record_source,record_load_dts)
	select distinct
		 hash_key
		,ProductID
		,record_source
		,record_load_dts
	from [stage].[matrix].[tbl_product]
	where not exists (select product_hash from [vault].[raw].[hub_product] where hub_product.product_hash = tbl_product.hash_key and hub_product.record_source = 'MATRIX')
	;

	insert into [vault].[raw].[sat_product]
			   (product_hash
			   ,product_id
			   ,[description]
			   ,record_source
			   ,hash_diff
			   )
	select distinct
		 tbl_product.hash_key
		,tbl_product.ProductID
		,tbl_product.ProductDesc
		,tbl_product.record_source
		,tbl_product.hash_diff

	from [stage].[matrix].[tbl_product]
	left outer join [vault].[raw].[sat_product] ON (
		sat_product.product_hash = tbl_product.hash_key
		and sat_product.record_load_end_dts IS NULL
		and sat_product.record_source = 'MATRIX')
	where	tbl_product.hash_diff != coalesce(sat_product.hash_diff, '')
	;

	update [vault].[raw].[sat_product]
	set record_load_end_dts = sat_max.record_load_dts
	from [vault].[raw].[sat_product] sat
	inner join
	(
		select product_hash,max(record_load_dts) as record_load_dts
		from [vault].[raw].[sat_product]
		where record_source = 'MATRIX'
		group by product_hash
	) as sat_max
	on sat.product_hash = sat_max.product_hash
	and sat.record_load_end_dts is null
	and sat.record_load_dts < sat_max.record_load_dts
	and sat.record_source = 'MATRIX'
	;

	insert into [vault].[raw].[hub_service]
			   (service_hash
			   ,service_id
			   ,record_source
			   ,record_load_dts)
	select distinct
		 tbl_service.hash_key
		,tbl_service.ServiceID
		,tbl_service.record_source
		,tbl_service.record_load_dts
	from [stage].[matrix].[tbl_service]
	where not exists (select hash_key from [vault].[raw].[hub_service] where hub_service.service_hash = tbl_service.hash_key and hub_service.record_source = 'MATRIX')

	insert into [vault].[raw].[sat_service]
			   (service_hash
			   ,service_id
			   ,[description]
			   ,Mon,Tue,Wed,Thu,Fri,Sat,Sun
			   ,record_source
			   ,hash_diff
			   )
	select distinct
		 tbl_service.hash_key
		,tbl_service.ServiceID
		,tbl_service.ServiceDesc
		,tbl_service.Mon,tbl_service.Tue,tbl_service.Wed,tbl_service.Thu,tbl_service.Fri,tbl_service.Sat,tbl_service.Sun
		,tbl_service.record_source
		,tbl_service.hash_diff

	from [stage].[matrix].[tbl_service]
	left outer join [vault].[raw].[sat_service] ON (
		sat_service.service_hash = tbl_service.hash_key
		and sat_service.record_load_end_dts IS NULL
		and sat_service.record_source = 'MATRIX')
	where	tbl_service.hash_diff != coalesce(sat_service.hash_diff, '')
	;

	update [vault].[raw].[sat_service]
	set record_load_end_dts = sat_max.record_load_dts
	from [vault].[raw].[sat_service] sat
	inner join
	(
		select service_hash,max(record_load_dts) as record_load_dts
		from [vault].[raw].[sat_service]
		where record_source = 'MATRIX'
		group by service_hash
	) as sat_max
	on sat.service_hash = sat_max.service_hash
	and sat.record_load_end_dts is null
	and sat.record_load_dts < sat_max.record_load_dts
	and sat.record_source = 'MATRIX'
	;

	insert into [vault].[raw].[hub_period]
			   (period_hash
			   ,period_id
			   ,record_source
			   ,record_load_dts)
	select distinct
		 tbl_period.hash_key
		,tbl_period.PeriodID
		,tbl_period.record_source
		,tbl_period.record_load_dts
	from [stage].[matrix].[tbl_period]
	where not exists (select period_hash from [vault].[raw].[hub_period] where hub_period.period_hash = tbl_period.hash_key and hub_period.record_source = 'MATRIX')
	;

	insert into [vault].[raw].[sat_period]
			   (period_hash
			   ,period_id
			   ,[description]
			   ,[type],issues_per_month,charge_cycle,first_month,first_day
			   ,record_source
			   ,hash_diff
			   )
	select distinct
		 tbl_period.hash_key
		,tbl_period.periodID
		,tbl_period.periodDesc
		,tbl_period.PeriodType,tbl_period.NumIssuesMonths,tbl_period.ChargeCycle,tbl_period.FirstMonth,tbl_period.FirstDay
		,tbl_period.record_source
		,tbl_period.hash_diff

	from [stage].[matrix].[tbl_period]
	left outer join [vault].[raw].[sat_period] ON (
			sat_period.period_hash = tbl_period.hash_key
		and sat_period.record_load_end_dts IS NULL
		and sat_period.record_source = 'MATRIX')

	where	tbl_period.hash_diff != coalesce(sat_period.hash_diff, '')
	;

	update [vault].[raw].[sat_period]
	set record_load_end_dts = sat_max.record_load_dts
	from [vault].[raw].[sat_period] sat
	inner join
	(
		select period_hash,max(record_load_dts) as record_load_dts
		from [vault].[raw].[sat_period]
		where record_source = 'MATRIX'
		group by period_hash
	) as sat_max
	on sat.period_hash = sat_max.period_hash
	and sat.record_load_end_dts is null
	and sat.record_load_dts < sat_max.record_load_dts
	and sat.record_source = 'MATRIX'
	;

	insert into [vault].[raw].[hub_cancellation_reason]
			   (cancellation_reason_hash
			   ,cancellation_reason_id
			   ,record_source
			   ,record_load_dts)
	select distinct
		 cancellation_reason.hash_key
		,cancellation_reason.canx_id
		,cancellation_reason.record_source
		,cancellation_reason.record_load_dts
	from [stage].[matrix].[cancellation_reason]
	where not exists (select cancellation_reason_hash from [vault].[raw].[hub_cancellation_reason] where hub_cancellation_reason.cancellation_reason_hash = cancellation_reason.hash_key and hub_cancellation_reason.record_source = 'MATRIX')
	;

	insert into [vault].[raw].[sat_cancellation_reason]
			   (cancellation_reason_hash
			   ,cancellation_reason_id
			   ,[description]
			   ,record_source
			   ,hash_diff
			   )
	select distinct
		 cancellation_reason.hash_key
		,cancellation_reason.canx_id
		,cancellation_reason.canx_desc
		,cancellation_reason.record_source
		,cancellation_reason.hash_diff

	from [stage].[matrix].[cancellation_reason]
	left outer join [vault].[raw].[sat_cancellation_reason] ON (
			sat_cancellation_reason.cancellation_reason_hash = cancellation_reason.hash_key
		and sat_cancellation_reason.record_load_end_dts IS NULL
		and sat_cancellation_reason.record_source = 'MATRIX')

	where	cancellation_reason.hash_diff != coalesce(sat_cancellation_reason.hash_diff, '')
	;

	update [vault].[raw].[sat_cancellation_reason]
	set record_load_end_dts = sat_max.record_load_dts
	from [vault].[raw].[sat_cancellation_reason] sat
	inner join
	(
		select cancellation_reason_hash,max(record_load_dts) as record_load_dts
		from [vault].[raw].[sat_cancellation_reason]
		where record_source = 'MATRIX'
		group by cancellation_reason_hash
	) as sat_max
	on sat.cancellation_reason_hash = sat_max.cancellation_reason_hash
	and sat.record_load_end_dts is null
	and sat.record_load_dts < sat_max.record_load_dts
	and sat.record_source = 'MATRIX'
	;

	-- Fix non ascii characters.
	update [stage].[matrix].[subscription] set sord_id = hexa(sord_id, PATINDEX('%[^0-9.]%', sord_id),1, '') where ISNUMERIC(sord_id) <> 1;
	delete from [stage].[matrix].[subscription] where sord_id is null;

	-- truncate table [vault].[raw].[link_subscription];
	insert into [vault].[raw].[link_subscription] (
		 subscription_hash
		,customer_hash
		,product_hash
		,service_hash
		,period_hash
		,subscription_id
		,record_source
		,record_load_dts
	)
	select distinct
		 subscription.hash_key
		,subscriber.hash_key as customer_hash
		,tbl_product.hash_key as product_hash
		,tbl_service.hash_key as service_hash
		,tbl_period.hash_key as period_hash

		-- this is not unique. remove? #todo
		,subscription.sord_id as subscription_id

		,subscription.record_source
		,subscription.record_load_dts

	-- select distinct top 100 subscription.sord_id,tbl_product.ProductID,tbl_service.ServiceID,tbl_period.PeriodID,hexa(subscription.sord_id, PATINDEX('%[^0-9.]%', subscription.sord_id),1, '') as subscription_id
	-- select count(1),count(distinct subscription.hash_key)
	from [stage].[matrix].[subscription]
	inner join [stage].[matrix].[tbl_service] on tbl_service.serviceID = subscription.serviceID
	inner join [stage].[matrix].[tbl_product] on tbl_product.productID = subscription.productID
	inner join [stage].[matrix].[tbl_period] on tbl_period.periodID = subscription.period_id
	inner join [stage].[matrix].[subscriber] on subscriber.subs_pointer = subscription.subs_pointer

	where not exists (select subscription_hash from [vault].[raw].[link_subscription] where link_subscription.subscription_hash = subscription.hash_key and link_subscription.record_source = 'MATRIX')
	;

	insert into [vault].[raw].[sat_subscription] (
				 subscription_hash
				,subscription_id
				,[type]
				,start_dt
				,end_dt
				,renewal_dt
				,campaign

				,record_source
				,hash_diff
			   )
	select distinct
		 subscription.hash_key
		,subscription.sord_id --sord_pointer
		,subscription.order_type
		,subscription.sord_startdate
		,subscription.sord_stopdate
		,subscription.exp_end_date
		,subscription.camp_id

		,subscription.record_source
		,subscription.hash_diff
	-- select count(1),count(distinct matrix_subscription.hash_key)
	from [stage].[matrix].[subscription]
	left outer join [vault].[raw].[sat_subscription] ON (
			sat_subscription.subscription_hash = subscription.hash_key
		and sat_subscription.record_load_end_dts IS NULL
		and sat_subscription.record_source = 'MATRIX')
	where	subscription.hash_diff != coalesce(sat_subscription.hash_diff, '')
	;
	-- select top 1000 * from [vault].[raw].[sat_subscription] where record_source = 'MATRIX';

	update [vault].[raw].[sat_subscription]
	set record_load_end_dts = sat_max.record_load_dts
	from [vault].[raw].[sat_subscription] sat
	inner join
	(
		select subscription_hash,max(record_load_dts) as record_load_dts
		from [vault].[raw].[sat_subscription]
		where record_source = 'MATRIX'
	group by subscription_hash
	) as sat_max
	on sat.subscription_hash = sat_max.subscription_hash
	and sat.record_load_end_dts is null
	and sat.record_load_dts < sat_max.record_load_dts
	and record_source = 'MATRIX'
	;

	-- truncate table [vault].[raw].[link_cancellation];
	insert into [vault].[raw].[link_cancellation]
	(
		 cancellation_hash
		,cancellation_reason_hash
		,customer_hash
		,product_hash
		,record_source
		,record_load_dts
	)
	select distinct
		 subord_cancel.hash_key as cancellation_hash

		,cancellation_reason.hash_key as cancellation_reason_hash
		,subscriber.hash_key as customer_hash
		,tbl_product.hash_key as product_hash


		,subord_cancel.[record_source]
		,subord_cancel.[record_load_dts]

	-- select count(1)
	from [stage].[matrix].[subord_cancel]
	inner join [stage].[matrix].[subscription] on subord_cancel.sord_pointer = subscription.sord_pointer and subord_cancel.ProductID = subscription.ProductID
	inner join [stage].[matrix].[subscriber] on subscriber.subs_pointer = subscription.subs_pointer
	inner join [stage].[matrix].[tbl_product] on tbl_product.productID = subscription.productID
	inner join [stage].[matrix].[cancellation_reason] on cancellation_reason.canx_id = subord_cancel.canx_id

	where not exists (select cancellation_hash from [vault].[raw].[link_cancellation] where link_cancellation.cancellation_hash = subord_cancel.hash_key and link_cancellation.record_source = 'MATRIX')
	;

	insert into [vault].[raw].[sat_cancellation] (
		 cancellation_hash
		,cancellation_dt
		,request_dts
		,sys_person_id
		,source_id
		,record_source
		,hash_diff
	)
	select distinct
		 subord_cancel.hash_key
		,subord_cancel.canx_date
		,subord_cancel.request_date
		,subord_cancel.syspersonid
		,subord_cancel.SourceID

		,subord_cancel.record_source
		,subord_cancel.hash_diff
	-- select count(1),count(distinct subord_cancel.hash_key)
	-- select top 100 *
	from [stage].[matrix].[subord_cancel]
	left outer join [vault].[raw].[sat_cancellation] ON (
			sat_cancellation.cancellation_hash = subord_cancel.hash_key
		and sat_cancellation.record_load_end_dts IS NULL
		and sat_cancellation.record_source = 'MATRIX')
	where	subord_cancel.hash_diff != coalesce(sat_cancellation.hash_diff, '')
	;
	-- select top 1000 * from [vault].[raw].sat_cancellation where record_source = 'MATRIX';

	update [vault].[raw].[sat_cancellation]
	set record_load_end_dts = sat_max.record_load_dts
	from [vault].[raw].[sat_cancellation] sat
	inner join
	(
		select cancellation_hash,max(record_load_dts) as record_load_dts
		from [vault].[raw].[sat_cancellation]
		where record_source = 'MATRIX'
		group by cancellation_hash
	) as sat_max
	on sat.cancellation_hash = sat_max.cancellation_hash
	and sat.record_load_end_dts is null
	and sat.record_load_dts < sat_max.record_load_dts
	and record_source = 'MATRIX'
	;

	insert into [stage].[log].[etl_job] ([user],[source],[job],[status]) values (CURRENT_USER,OBJECT_SCHEMA_NAME(@@PROCID),OBJECT_NAME(@@PROCID),'finished');
GO

````
