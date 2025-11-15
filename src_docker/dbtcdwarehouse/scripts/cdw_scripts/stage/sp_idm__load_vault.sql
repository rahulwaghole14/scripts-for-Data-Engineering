SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [idm].[load_vault] AS

	-- Usage
	-- exec [stage].[idm].[load_vault];

	insert into [stage].[log].[etl_job] ([user],[source],[job],[status]) values (CURRENT_USER,OBJECT_SCHEMA_NAME(@@PROCID),OBJECT_NAME(@@PROCID),'started');

	/*
	delete from [vault].[raw].[sat_customer] where record_source = 'IDM';
	delete from [vault].[raw].[hub_customer] where record_source = 'IDM';
	*/

    -- rebuild index on the 'marketing_id' column
	ALTER INDEX idx_marketing_id ON stage.idm.drupal__user_profiles REBUILD;

    -- INSERT new customer records into hub_customer table in vault.raw
    with users as (
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
        , record_source
        , record_load_dts_utc as record_load_dts
    from [stage].[idm].[drupal__user_profiles]
    )

	insert into [vault].[raw].[hub_customer] (customer_hash,customer_id,record_source,record_load_dts)
    select * from users
    where not exists (select customer_hash from [vault].[raw].[hub_customer] where hub_customer.customer_hash = users.hash_key and hub_customer.record_source = 'IDM')
    ;

	-- loading new customer record into scv delta
	-- exec [delta].[load_scv_import] 'idm'
	;

	with users as (
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
		, CAST(YEAR(year_of_birth) AS SMALLINT) as year_of_birth
		, email
		, RIGHT(REPLACE(REPLACE( contact_phone,'()',''),'(+',''),32) as home_phone
		, LEFT( contact_phone,32) as mobile_phone
		, LEFT( street_address,128) as address_2
		, locality as address_3
		, city_region as address_4
		, country
		, record_source
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
		, year_of_birth
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

	insert into [vault].[raw].[sat_customer] (
		 customer_hash
		,customer_id
		,first_name
		,last_name
		,yob
		,email
		,phone_home
		,phone_mobile
		,address_2
		,address_3
		,address_4
		,country
		,record_source
		,record_load_dts
		,hash_diff
	)

    select hash_diff.* from hash_diff
	left outer join [vault].[raw].[sat_customer] ON (
					sat_customer.customer_hash = hash_diff.hash_key
				and sat_customer.record_load_end_dts IS NULL
				and sat_customer.record_source = 'IDM'
				)
	where hash_diff.hash_diff != coalesce(sat_customer.hash_diff, '')
	;

	-- select top 100 * from [vault].[raw].[sat_customer] where record_source = 'IDM';
	-- select count(1),count(distinct customer_hash) from [vault].[raw].[sat_customer] where record_source = 'IDM';

	update [vault].[raw].[sat_customer]
	set record_load_end_dts = sat_max.record_load_dts
	from [vault].[raw].[sat_customer] sat
	inner join
	(
		select customer_hash,max(record_load_dts) as record_load_dts
		from [vault].[raw].[sat_customer]
		where record_source = 'IDM'
		group by customer_hash
	) as sat_max
	on sat.customer_hash = sat_max.customer_hash
	and sat.record_load_end_dts is null
	and sat.record_load_dts < sat_max.record_load_dts
	and sat.record_source = 'IDM'
	;


	insert into [stage].[log].[etl_job] ([user],[source],[job],[status]) values (CURRENT_USER,OBJECT_SCHEMA_NAME(@@PROCID),OBJECT_NAME(@@PROCID),'finished');
GO
