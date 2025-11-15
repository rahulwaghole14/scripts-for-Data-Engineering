SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [delta].[load_scv_import] @record_source varchar(20) AS

	-- Usage
	-- exec [stage].[delta].[load_scv_import] @record_source;


	insert into [stage].[log].[etl_job] ([user],[source],[job],[status]) values (CURRENT_USER,OBJECT_SCHEMA_NAME(@@PROCID),OBJECT_NAME(@@PROCID),'started');

	/*
	delete from [vault].[raw].[sat_customer] where record_source = 'FIBRE';
	delete from [vault].[raw].[hub_customer] where record_source = 'FIBRE';
	delete from [vault].[raw].[sat_payment] where record_source = 'FIBRE';
	delete from [vault].[raw].[hub_payment] where record_source = 'FIBRE';
	delete from [vault].[raw].[sat_product] where record_source = 'FIBRE';
	delete from [vault].[raw].[hub_product] where record_source = 'FIBRE';
	delete from [vault].[raw].[link_subscription] where record_source = 'FIBRE';
	delete from [vault].[raw].[sat_subscription] where record_source = 'FIBRE';
	*/

	alter index all on [scv].[delta].[SCV_delta] disable;

/* Decommission on 2020 July 05
	-- loading delta records source from Fibre
	if lower(@record_source) = 'fibre' begin

	insert into [scv].[delta].[SCV_delta] (
		 customer_hash
		,customer_id
		,[type]
		-- ,active
		,[status]
		,first_name
		-- ,middle_name
		,last_name
		--,dob
		,email --,email_alternative
		,phone_home
		--,phone_work
		,phone_mobile
		,address_type
		,address_2,address_3,address_4
		-- ,dpid
		,latitude,longitude
		,country
		,record_source,record_load_dts,hash_diff
	)
	select
		 customer.hash_key
		,customer.customer_id
		,customer.[type]
		,customer.[status]
		,customer.first_name
		,customer.last_name
		,customer.email
		,customer.main_number
		,customer.number_mobile
		,customer.address_type
		,nullif(concat_ws(' ',concat(nullif(concat(number_pre,'/'),'/'),number,number_post),street_name,street_type),'') as address_2
		,suburb as address_3
		,nullif(concat_ws(' ',city,postcode),'')  as address_4
		,customer.lat,customer.long
		,customer.country
		,rtrim(customer.record_source) as record_source
		,customer.record_load_dts
		,customer.hash_diff

	from [stage].[fibre].[customer]
		left outer join [vault].[raw].[sat_customer] ON (
						sat_customer.customer_hash = customer.hash_key
					and sat_customer.record_load_end_dts IS NULL
					and sat_customer.record_source = 'FIBRE'
					)
        left outer join [scv].[delta].[SCV_delta] ON (
		[scv].[delta].[SCV_delta].customer_hash = customer.hash_key)
	where customer.hash_diff != coalesce(sat_customer.hash_diff, '')
	and customer.hash_diff != coalesce([scv].[delta].[SCV_delta].hash_diff,'')
	end
*/

		-- loading delta records source from hexaEVENT
	if lower(@record_source) = 'hexaevent' begin
		insert into [scv].[delta].[SCV_delta] (
		 customer_hash
		,customer_id
		,[type]
		,[status]
		,first_name
		,last_name
		,email
		,phone_home
		,phone_mobile
		,address_1
		,address_2
		,address_3
		,address_4
		,country
		,record_source
		,record_load_dts
		,hash_diff
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
		,'New Zealand'
		,[cust_detail].record_source
		,[cust_detail].record_load_dts
		,[cust_detail].hash_diff

		-- select count(1),count(distinct [cust_detail].[generated_cust_id])
		-- select top 1000 *
	from [stage].[hexaEVENT].[cust_detail]
	left outer join [vault].[raw].[sat_customer] ON (
					sat_customer.customer_hash = [cust_detail].hash_key
				and sat_customer.record_load_end_dts IS NULL
				and sat_customer.record_source = 'hexaEVENT'
				)
	left outer join [scv].[delta].[SCV_delta] ON (
		[scv].[delta].[SCV_delta].customer_hash = [cust_detail].hash_key)

	where	[cust_detail].hash_diff != coalesce(sat_customer.hash_diff, '')
	and [cust_detail].hash_diff != coalesce([scv].[delta].[SCV_delta].hash_diff,'')
	end

		-- loading delta records source from IDM
	if lower(@record_source) = 'idm' begin

		with existing as (
			select
				customer_hash
				, record_source
				, record_load_dts
			from [scv].[delta].[SCV_delta]
    	)
		, users as (
			select distinct
				LOWER(
					CONVERT(NVARCHAR(32),
					HASHBYTES('MD5',
					CONVERT(VARBINARY(MAX),
					CONVERT(VARCHAR(MAX), 'IDM' + '|' + CONVERT(NVARCHAR(20), user_id))
					COLLATE SQL_Latin1_General_CP1_CS_AS))
					, 2)
				) AS hash_key
				, user_id
				, first_name
				, last_name
				, email
				, RIGHT(REPLACE(REPLACE( contact_phone,'()',''),'(+',''),32) as home_phone
				, LEFT( contact_phone,32) as mobile_phone
				, LEFT( street_address,128) as address_2
				, locality as address_3
				, city_region as address_4
				, country
				, RTRIM( record_source) as record_source
				, CONCAT(adobe_id,subscriber_id,username,active,country,postcode,street_address,display_name,emails_primary
					,emails_type,email,resource_type,last_name,first_name,phone_type,contact_phone,timezone,user_consent
					,email_verified,gender,mobile_verified,date_of_birth,created_date,last_modified,verified_date,mobile_verified_date
					,year_of_birth,city_region,locality
					) as concat_diff
				, record_load_dts_utc as record_load_dts
				, hash_diff
			from [stage].[idm].[drupal__user_profiles]
			),

		hash_diff as (
			select
			hash_key
			, user_id
			, first_name
			, last_name
			, email
			, home_phone
			, mobile_phone
			, address_2
			, address_3
			, address_4
			, country
			, record_source
			, record_load_dts
			, COALESCE( hash_diff, LOWER(
					CONVERT(NVARCHAR(32),
					HASHBYTES('MD5',
					CONVERT(VARBINARY(MAX),
					CONVERT(VARCHAR(MAX), 'IDM' + '|' + CONVERT(NVARCHAR, concat_diff))
					COLLATE SQL_Latin1_General_CP1_CS_AS))
					, 2)
				)) AS hash_diff
			from users
		)

		insert into [scv].[delta].[SCV_delta] (
		  customer_hash
		, customer_id
		, first_name
		, last_name
		, email
		, phone_home
		, phone_mobile
		, address_2
		, address_3
		, address_4
		, country
		, record_source
		, record_load_dts
		, hash_diff
		)

		select
			  hash_diff.hash_key
			, hash_diff.user_id
			, hash_diff.first_name
			, hash_diff.last_name
			, hash_diff.email
			, hash_diff.home_phone
			, hash_diff.mobile_phone
			, hash_diff.address_2
			, hash_diff.address_3
			, hash_diff.address_4
			, hash_diff.country
			, hash_diff.record_source
			, hash_diff.record_load_dts
			, hash_diff.hash_diff
		from hash_diff
		left outer join [vault].[raw].[sat_customer] ON (
						sat_customer.customer_hash = hash_diff.hash_key
					and sat_customer.record_load_end_dts IS NULL
					and sat_customer.record_source = 'IDM'
					)
		left outer join [scv].[delta].[SCV_delta] ON (
			[scv].[delta].[SCV_delta].customer_hash = hash_diff.hash_key)
        left join existing e on (
            e.customer_hash = hash_diff.hash_key
            and e.record_load_dts = hash_diff.record_load_dts
            and e.record_source = hash_diff.record_source
        )
		where hash_diff.hash_diff != coalesce(sat_customer.hash_diff, '')
		and hash_diff.hash_diff != coalesce([scv].[delta].[SCV_delta].hash_diff,'')
		and e.customer_hash is null

	end

		-- loading delta records source from MATRix
	if lower(@record_source) = 'matrix' begin

	with existing as (
		select
			customer_hash
			, record_source
			, record_load_dts
		from [scv].[delta].[SCV_delta]
	)

	, new_records as (
			select distinct customer.hash_key
				, customer.subs_id
				, customer.title,customer.FirstName,customer.SurName,customer.PersContact3
				, customer.HOME,customer.WORK,customer.MOBILE
				, customer.AddrLine3
				, customer.AddrLine4
				, customer.AddrLine5
				, customer.AddrLine6
				, customer.CountryName
				, rtrim(customer.record_source) as record_source
				, customer.record_load_dts
				, customer.hash_diff
			from [stage].[matrix].[customer]
			left outer join [vault].[raw].[sat_customer] ON (
							sat_customer.customer_hash = customer.hash_key
						and sat_customer.record_load_end_dts IS NULL
						and sat_customer.record_source = 'MATRIX'
						)
			left outer join [scv].[delta].[SCV_delta] ON (
				[scv].[delta].[SCV_delta].customer_hash = customer.hash_key
			)
	        left join existing e on (
				e.customer_hash = customer.hash_key
				and e.record_load_dts = customer.record_load_dts
				and e.record_source = rtrim(customer.record_source)
   		     )
			where customer.hash_diff != coalesce(sat_customer.hash_diff, '')
			and customer.hash_diff != coalesce([scv].[delta].[SCV_delta].hash_diff,'')
			and e.customer_hash is null
		)

	insert into [scv].[delta].[SCV_delta] (
		customer_hash
		, customer_id
		, title
		, first_name
		, last_name
		, email
		, phone_home
		, phone_work
		, phone_mobile
		, address_1
		, address_2
		, address_3
		, address_4
		, country
		, record_source
		, record_load_dts
		, hash_diff
	)

	SELECT * FROM new_records
    end

		-- loading delta records source from neighbourly
	if lower(@record_source) = 'neighbourly' begin
	insert into [scv].[delta].[SCV_delta] (
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
		, [user].id
		, [user].is_active
		, nullif([user].first_name,'')
		, nullif([user].last_name,'')
		, substring([user].email,0,128)
		, [user].mobile
		, [paf_address].address_type
		, nullif(trim(trim(trim([paf_address].[unit_type] + ' ' + [paf_address].[unit_identifier]) + ' ' + [paf_address].[floor]) + ' ' + [paf_address].[building_name]),'') as address_1
		, nullif(trim(trim(cast([paf_address].[street_number] as varchar) + [paf_address].[street_alpha] + ' ' + [paf_address].[street_name]) + ' ' + [paf_address].[street_type]),'') as address_2
		, nullif([suburb_name],'') as address_3
		, nullif(trim([town_city_mailtown] + ' ' + [postcode]),'') as address_4
		, rtrim([user].record_source) as record_source
		, [user].record_load_dts
		, [user].hash_diff
	from [stage].[neighbourly].[user]
	left join (select user_id,max(id) id from [stage].[neighbourly].[user_paf_address] group by user_id ) as max_paf on [user].id = max_paf.user_id
	left join [stage].[neighbourly].[user_paf_address] on max_paf.id = [user_paf_address].id
	left join [stage].[neighbourly].[paf_address] on [paf_address].id = [user_paf_address].[paf_address_id]
	left outer join [vault].[raw].[sat_customer] ON (
					sat_customer.customer_hash = [user].hash_key
				and sat_customer.record_load_end_dts IS NULL
				and sat_customer.record_source = 'neighbourly'
				)
	left outer join [scv].[delta].[SCV_delta] ON (
		[scv].[delta].[SCV_delta].customer_hash = [user].hash_key)
	where	[user].hash_diff != coalesce(sat_customer.hash_diff, '')
		and [user_paf_address].moved_out_at is null
		and [user].hash_diff != coalesce([scv].[delta].[SCV_delta].hash_diff,'')
    end

alter index all on [scv].[delta].[SCV_delta] rebuild;

insert into [stage].[log].[etl_job] ([user],[source],[job],[status]) values (CURRENT_USER,OBJECT_SCHEMA_NAME(@@PROCID),OBJECT_NAME(@@PROCID),'finished');
GO
