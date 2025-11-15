SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE   PROCEDURE [presspatron].[load_vault] AS

	-- Usage
	-- exec [stage].[PRESSPATRON].[load_vault];

insert into [stage].[log].[etl_job] ([user],[source],[job],[status]) values (CURRENT_USER,OBJECT_SCHEMA_NAME(@@PROCID),OBJECT_NAME(@@PROCID),'started');

/*
	delete from [vault].[raw].[sat_customersupporter] where record_source = 'PRESSPATRON';
	delete from [vault].[raw].[sat_customer] where record_source = 'PRESSPATRON';
	delete from [vault].[raw].[hub_customer] where record_source = 'PRESSPATRON';
	delete from [vault].[raw].[sat_payment] where record_source = 'PRESSPATRON';
	delete from [vault].[raw].[hub_payment] where record_source = 'PRESSPATRON';
	delete from [vault].[raw].[sat_period] where record_source = 'PRESSPATRON';
	delete from [vault].[raw].[hub_period] where record_source = 'PRESSPATRON';
	delete from [vault].[raw].[link_donation] where record_source = 'PRESSPATRON';
	delete from [vault].[raw].[sat_donation] where record_source = 'PRESSPATRON';
*/

/* */

	insert into [vault].[raw].[hub_customer] (customer_hash,customer_id,record_source,record_load_dts)
	select distinct
		   hash_key
		  ,transactionid
		  ,record_source
		  ,record_load_dts
	from [stage].[PRESSPATRON].[supporter]
	where not exists (select customer_hash from [vault].[raw].[hub_customer]
						where hub_customer.customer_hash = supporter.hash_key and hub_customer.record_source = 'PRESSPATRON')
	;

	insert into [vault].[raw].[sat_customer] (
		 customer_hash
		,customer_id
		,[type]
		,active
		--,[status]
		,first_name
		-- ,middle_name
		,last_name
		--,dob
		,email --,email_alternative
		--,phone_home
		--,phone_work
		--,phone_mobile
		--,address_type
		--,address_2,address_3,address_4
		-- ,dpid
		--,latitude,longitude
		--,country
		,record_source,record_load_dts,hash_diff
	)

	select distinct
		 supporter.hash_key
		,supporter.transactionid
		,'supporter'
		,case when supporter.[Active supporter?] = 'Yes' then 1
		 else 0
		 end
		,supporter.[first name]
		,supporter.[last name]
		,supporter.[email address]
		,supporter.record_source
		,supporter.record_load_dts
		,supporter.hash_diff

	from [stage].[PRESSPATRON].[supporter]
		left outer join [vault].[raw].[sat_customer] ON (
						sat_customer.customer_hash = supporter.hash_key
					and sat_customer.record_load_end_dts IS NULL
					and sat_customer.record_source = 'PRESSPATRON'
					)
	where	supporter.hash_diff != coalesce(sat_customer.hash_diff, '')
	;
	-- select top 100 * from [vault].[raw].[sat_customer] where record_source = 'PRESSPATRON';
	-- select count(1),count(distinct customer_hash) from [vault].[raw].[sat_customer] where record_source = 'PRESSPATRON';

	update [vault].[raw].[sat_customer]
	set record_load_end_dts = sat_max.record_load_dts
	from [vault].[raw].[sat_customer] sat
	inner join
	(
		select customer_hash,max(record_load_dts) as record_load_dts
		from [vault].[raw].[sat_customer]
		where record_source = 'PRESSPATRON'
		group by customer_hash
	) as sat_max
	on sat.customer_hash = sat_max.customer_hash
	and sat.record_load_end_dts is null
	and sat.record_load_dts < sat_max.record_load_dts
	and sat.record_source = 'PRESSPATRON'
	;

		insert into [vault].[raw].[sat_customersupporter] (
		 customersupporter_hash
		,customer_id
		,[signup_dt]
		,[type]
		,active
		,newsletter_subscriberYN
		,total_contribution
		,record_source,record_load_dts,hash_diff
	)

	select
		 supporter.hash_key
		,supporter.transactionid
		,supporter.[sign up date]
		,supporter.[Frequency of recurring payment]
		,case when supporter.[Active supporter?] = 'Yes' then 1
		 else 0
		 end
		,Left(supporter.[subscribed to newsletter?],1)
		,supporter.[total contributions to date]
		,supporter.record_source
		,supporter.record_load_dts
		,supporter.hash_diff

	from [stage].[PRESSPATRON].[supporter]
		left outer join [vault].[raw].[sat_customersupporter] ON (
						sat_customersupporter.customersupporter_hash = supporter.hash_key
					and sat_customersupporter.record_load_end_dts IS NULL
					and sat_customersupporter.record_source = 'PRESSPATRON'
					)
	where	supporter.hash_diff != coalesce(sat_customersupporter.hash_diff, '')
	;
	-- select top 100 * from [vault].[raw].[sat_customersupporter] where record_source = 'PRESSPATRON';
	-- select count(1),count(distinct customer_hash) from [vault].[raw].[sat_customersupporter] where record_source = 'PRESSPATRON';

	update [vault].[raw].[sat_customersupporter]
	set record_load_end_dts = sat_max.record_load_dts
	from [vault].[raw].[sat_customersupporter] sat
	inner join
	(
		select customersupporter_hash,max(record_load_dts) as record_load_dts
		from [vault].[raw].[sat_customersupporter]
		where record_source = 'PRESSPATRON'
		group by customersupporter_hash
	) as sat_max
	on sat.customersupporter_hash = sat_max.customersupporter_hash
	and sat.record_load_end_dts is null
	and sat.record_load_dts < sat_max.record_load_dts
	and sat.record_source = 'PRESSPATRON'
	;


	insert into [vault].[raw].[hub_payment] (payment_hash,payment_id,record_source,record_load_dts)
	select distinct
		   hash_key
		  ,transactionid
		  ,record_source
		  ,record_load_dts
	from [stage].[PRESSPATRON].[payment]
	where not exists (select payment_hash from [vault].[raw].[hub_payment] where hub_payment.payment_hash = payment.hash_key and hub_payment.record_source = 'PRESSPATRON')
	;

	insert into [vault].[raw].[sat_payment] (
		 payment_hash
		,payment_id
		,[type]
		,amount
		,[description]
		,payment_dt
		,record_source
		,record_load_dts
		,hash_diff
	)
	select distinct
		 payment.hash_key
		,payment.transactionid
		,Left(payment.[payment status],12)
		,payment.[amount of transaction]
		,'Recurring payment: ' + cast(coalesce(payment.[nbr_of_payment],0) as varchar(10)) as [description]
		,payment.[transaction date]
		,payment.record_source
		,payment.record_load_dts
		,payment.hash_diff
	from [stage].[PRESSPATRON].[payment]
		left outer join [vault].[raw].[sat_payment] ON (
						sat_payment.payment_hash = payment.hash_key
					and sat_payment.record_load_end_dts IS NULL
					and sat_payment.record_source = 'PRESSPATRON'
					)
	where	payment.hash_diff != coalesce(sat_payment.hash_diff, '')
	;
	-- select top 100 * from [vault].[raw].[sat_payment] where record_source = 'PRESSPATRON';

	update [vault].[raw].[sat_payment]
	set record_load_end_dts = sat_max.record_load_dts
	from [vault].[raw].[sat_payment] sat
	inner join
	(
		select payment_hash,max(record_load_dts) as record_load_dts
		from [vault].[raw].[sat_payment]
		where record_source = 'PRESSPATRON'
		group by payment_hash
	) as sat_max
	on sat.payment_hash = sat_max.payment_hash
	and sat.record_load_end_dts is null
	and sat.record_load_dts < sat_max.record_load_dts
	and sat.record_source = 'PRESSPATRON'
	;

	insert into [vault].[raw].[hub_period] (period_hash,period_id,record_source,record_load_dts)
	select distinct
		   hash_key
		  ,Frequency
		  ,record_source
		  ,record_load_dts
	from [stage].[PRESSPATRON].[period]
	where not exists (select period_hash from [vault].[raw].[hub_period]
	                  where hub_period.period_hash = period.hash_key and hub_period.record_source = 'PRESSPATRON')
	;
	-- select * from [vault].[raw].[hub_period] where record_source = 'PRESSPATRON';

	insert into [vault].[raw].[sat_period]
			   (period_hash
				,period_id
				,[description]
				--,[type]
				--,price
				--,retail_price
				--,active
				--,[rank]
				,recurring
				,record_source
				,record_load_dts
				,hash_diff
			   )
	select distinct
		 period.hash_key
		,period.Frequency
		,period.Frequency +' '+ 'donation'
		,Case when Frequency = 'One-time' Then 'N' Else 'Y' End
		,period.record_source
		,period.record_load_dts
		,period.hash_diff

	from [stage].[PRESSPATRON].[period]
		left outer join [vault].[raw].[sat_period] ON (
						sat_period.period_hash = period.hash_key
					and sat_period.record_load_end_dts IS NULL
					and sat_period.record_source = 'PRESSPATRON'
					)
	where	period.hash_diff != coalesce(sat_period.hash_diff, '')
	;
	-- select * from [vault].[raw].[sat_period] where record_source = 'PRESSPATRON';

	update [vault].[raw].[sat_period]
	set record_load_end_dts = sat_max.record_load_dts
	from [vault].[raw].[sat_period] sat
	inner join
	(
		select period_hash,max(record_load_dts) as record_load_dts
		from [vault].[raw].[sat_period]
		where record_source = 'PRESSPATRON'
		group by period_hash
	) as sat_max
	on sat.period_hash = sat_max.period_hash
	and sat.record_load_end_dts is null
	and sat.record_load_dts < sat_max.record_load_dts
	and sat.record_source = 'PRESSPATRON'
	;

	insert into [vault].[raw].[link_donation] (
		 donation_hash
		,customer_hash
		,period_hash
	--	,service_hash
		,payment_hash
		,donation_id
		,record_source
		,record_load_dts
	)
	select distinct
		 [fst_don].hash_key
		,cust_supporter.hash_key as customer_hash
		,period.hash_key as period_hash
 		,payment.hash_key as payment_hash
		,[payment].transactionid as donation_id
		,[donation].record_source
		,[donation].record_load_dts
	from [stage].[PRESSPATRON].[donation]
	inner join [stage].[PRESSPATRON].[period]
	on ([period].frequency = [donation].frequency)
	inner join (	select -- donation.*
						distinct
						[donation].sequence_nr
						,max(supporter.hash_key) as hash_key
						from [stage].[PRESSPATRON].[donation] donation
						inner join (
									select  distinct hash_key,
											[email address],
											[sign up date],
											coalesce([Frequency of recurring payment],'One-time') as Frequency,
											case when lower([Active Supporter?]) = 'yes'
											then Replace([Recurring contribution amount?],'.0','') + '.0'
											else
											[Total contributions to date]
											end as [Recurring_contribution]
									from [stage].[PRESSPATRON].[supporter]
									) supporter
									on (coalesce(supporter.[email address],'no-email')+supporter.[sign up date]+ lower(supporter.[Frequency])
									= coalesce([donation].[email address],'no-email') + donation.[sign up date] + lower(donation.[Frequency])
									) group by	[donation].sequence_nr

		) cust_supporter
		on (cust_supporter.sequence_nr = donation.sequence_nr)
		inner join
	  (
		select donation.[sequence_nr], snr, min_don.hash_key from [stage].[presspatron].[donation]
              inner join (
							select [Email address],[Sign up date],[Frequency],[Amount of transaction], min(hash_key) as hash_key, min(sequence_nr) as snr
							from [stage].[presspatron].[donation]
							group by [Email address],[Sign up date],[Frequency],[Amount of transaction]
							) min_don
							on (
							donation.[Email address] = min_don.[Email address] and donation.[Sign up date] = min_don.[Sign up date] and donation.[Frequency] = min_don.[Frequency]
							and donation.[Amount of transaction] = min_don.[Amount of transaction]
							)
--order by 2,1
		) fst_don
	on (donation.[sequence_nr] = fst_don.[sequence_nr])
	left join [stage].[PRESSPATRON].[payment]
	on (payment.[sequence_nr] = fst_don.[sequence_nr])
	where not exists (select 1 from [vault].[raw].[link_donation] where link_donation.donation_hash = [donation].hash_key and link_donation.record_source = 'PRESSPATRON')
	;

	--  select count(1),count(distinct donation_hash) from [vault].[raw].[link_donation] where record_source = 'PRESSPATRON';
	--  select top 1000 * from [vault].[raw].[link_donation] where record_source = 'PRESSPATRON' order by 2;

	insert into [vault].[raw].[sat_donation] (
		 donation_hash
		,donation_id
		,[status]
		,start_dt
		-- ,cancellled_dt
		,end_dt
		-- ,renewal_dt
		,campaign
		,url_source
		,record_source
		,hash_diff
	)
	select distinct
		 stage_donation.hash_key as donation_hash
		,stage_donation.transactionid as donation_id
		,stage_donation.[Frequency] as [status]
		,stage_donation.[sign up date] as start_dt
		-- ,convert(date,service.end_date,120) -- always null?
		,stage_donation.[latest transaction dt] as end_dt
		,Replace(stage_donation.min_Metadata,'-0',stage_donation.max_Metadata) as campaign
		,Replace(stage_donation.min_url,'-0', max_url) as url_source
		--,price?
		,stage_donation.record_source as record_source
		,stage_donation.hash_diff as hash_diff
	-- select count(1),count(distinct hash_key)
	from (select min([donation].hash_key) as hash_key
		,min([payment].[transactionid]) as transactionid
		,[donation].[Frequency] as Frequency
		,convert(date,[donation].[sign up date],120) as [sign up date]
		,max(convert(date,[donation].[transaction date],120)) as [latest transaction dt]
		,min(coalesce([donation].Metadata,'-0')) as min_metadata
		,max(coalesce([donation].Metadata,'')) as max_Metadata
		,min(coalesce([donation].[url Source],'-0')) as min_url
		,max(coalesce([donation].[url Source],'')) as max_url
		,min([donation].record_source) as record_source
		,min([donation].hash_diff) as hash_diff
		from [stage].[PRESSPATRON].[donation] inner join
		(select distinct sequence_nr, transactionid from [stage].[PRESSPATRON].[payment]) payment
		on donation.sequence_nr = payment.sequence_nr
		group by [Email address],[sign up date],[Frequency],[amount of transaction]) stage_donation
	left outer join [vault].[raw].[sat_donation] ON (
					sat_donation.donation_hash = stage_donation.hash_key
				and sat_donation.record_load_end_dts IS NULL
				and sat_donation.record_source = 'PRESSPATRON'
				)
	where	stage_donation.hash_diff != coalesce(sat_donation.hash_diff, '')
	;

	--  select count(1),count(distinct donation_hash) from [vault].[raw].[sat_donation] where record_source = 'PRESSPATRON';
	--  select top 1000 * from [vault].[raw].[sat_donation] where record_source = 'PRESSPATRON' order by 2;

	update [vault].[raw].[sat_donation]
	set record_load_end_dts = sat_max.record_load_dts
	from [vault].[raw].[sat_donation] sat
	inner join
	(
		select donation_hash,max(record_load_dts) as record_load_dts
		from [vault].[raw].[sat_donation]
		where record_source = 'PRESSPATRON'
		group by donation_hash
	) as sat_max
		on sat.donation_hash = sat_max.donation_hash
	and sat.record_load_end_dts is null
	and sat.record_load_dts < sat_max.record_load_dts
	and record_source = 'PRESSPATRON'
	;

insert into [stage].[log].[etl_job] ([user],[source],[job],[status]) values (CURRENT_USER,OBJECT_SCHEMA_NAME(@@PROCID),OBJECT_NAME(@@PROCID),'finished');
GO
