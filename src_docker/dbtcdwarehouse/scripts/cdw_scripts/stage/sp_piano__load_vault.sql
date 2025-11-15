CREATE OR ALTER PROCEDURE [load_mart].[print_digi_comparison]
AS
BEGIN
    IF OBJECT_ID('mart.print_digital_comparison', 'U') IS NOT NULL
    DROP TABLE mart.print_digital_comparison;

    SELECT * INTO mart.print_digital_comparison
    FROM vault.mart.print_digi_comparison;
END
GO
;
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE OR ALTER PROCEDURE [piano].[load_vault] AS

	insert into [stage].[log].[etl_job] ([user],[source],[job],[status]) values (CURRENT_USER,OBJECT_SCHEMA_NAME(@@PROCID),OBJECT_NAME(@@PROCID),'started');


-- drop all data
  	-- delete from [vault].[raw].[link_cancellation] where record_source = 'PIANO'
	-- delete from [vault].[raw].[link_subscription] where record_source = 'PIANO'
	-- delete from [vault].[raw].[hub_product] where record_source = 'PIANO'
	-- delete from [vault].[raw].[hub_customer] where record_source = 'PIANO'
	-- delete from [vault].[raw].[sat_product] where record_source = 'PIANO'
	-- delete from [vault].[raw].[sat_customer] where record_source = 'PIANO'
	-- delete from [vault].[raw].[sat_subscription] where record_source = 'PIANO'
	-- delete from [vault].[raw].[sat_cancellation] where record_source = 'PIANO'
-- HUB_CUSTOMER

    WITH ranked_records AS (
        SELECT
            user__uid,
            user__email,
            UPPER(record_source) as record_source,
            record_load_dts,
            ROW_NUMBER() OVER (
                PARTITION BY user__uid, user__email
                ORDER BY record_load_dts DESC
            ) as rn
        FROM
            [stage].[piano].[piano__subscriptions]
    )

    , piano_customer_data as (
        SELECT
            user__uid as customer_id,
            user__email,
            record_source,
            record_load_dts
        FROM
            ranked_records
        WHERE
            rn = 1
    )

    , add_hash as (
        SELECT
            *
            , CAST( LOWER(
                CONVERT(NVARCHAR(32),
                HASHBYTES('MD5',
                CONVERT(VARBINARY(MAX),
                CONVERT(VARCHAR(MAX), record_source  + '|' + customer_id + '|' + user__email )
                COLLATE SQL_Latin1_General_CP1_CS_AS))
                , 2)
            ) AS CHAR(32)) AS customer_hash
        FROM piano_customer_data
    )

	insert into [vault].[raw].[hub_customer] (customer_hash,customer_id,record_source,record_load_dts)

    select customer_hash
        , customer_id
        , record_source
        , record_load_dts
    from add_hash
    where not exists (select customer_hash from [vault].[raw].[hub_customer] where hub_customer.customer_hash = add_hash.customer_hash and hub_customer.record_source = 'PIANO')
    ;

-- sat_customer piano

    WITH ranked_records AS (
        SELECT
            ISNULL(user__uid,'') as user__uid
            , ISNULL(user__email,'') as user__email
            , ISNULL(user__first_name,'') as user__first_name
            , ISNULL(user__last_name,'') as user__last_name
            , ISNULL(UPPER(record_source),'') as record_source
            , ISNULL(record_load_dts,'') as record_load_dts
            , ROW_NUMBER() OVER (
                PARTITION BY user__uid, user__email
                ORDER BY record_load_dts DESC
            ) as rn
        FROM
            [stage].[piano].[piano__subscriptions]
    )

    , piano_customer_data as (
        SELECT
            user__uid as customer_id
            , user__email
            , user__first_name
            , user__last_name
            , record_source
            , record_load_dts
        FROM
            ranked_records
        WHERE
            rn = 1
    )

    , add_hash as (
        SELECT
            *
            , CAST( LOWER(
                CONVERT(NVARCHAR(32),
                HASHBYTES('MD5',
                CONVERT(VARBINARY(MAX),
                CONVERT(VARCHAR(MAX), record_source  + '|' + customer_id + '|' + user__email )
                COLLATE SQL_Latin1_General_CP1_CS_AS))
                , 2)
            ) AS CHAR(32)) AS hash_key
            , CAST( LOWER(
                CONVERT(NVARCHAR(32),
                HASHBYTES('MD5',
                CONVERT(VARBINARY(MAX),
                CONVERT(VARCHAR(MAX)
                , customer_id +
                user__email +
                user__first_name +
                user__last_name +
                record_source
                )
                COLLATE SQL_Latin1_General_CP1_CS_AS))
                , 2)
            ) AS CHAR(32)) AS hash_diff
        FROM piano_customer_data
    )

	insert into [vault].[raw].[sat_customer] (
		 customer_hash
		,customer_id
		,[type]
		,title
		,first_name
		,last_name
		,email
		,phone_home,phone_work,phone_mobile
		,address_1,address_2,address_3,address_4
		,country
		,record_source
		,record_load_dts
		,hash_diff
	)

    select
            hash_key as customer_hash
            , add_hash.customer_id as customer_id
            , null as type
            , null as title
            , user__first_name as first_name
            , user__last_name as last_name
            , user__email as email
            , null as phone_home
            , null as phone_work
            , null as phone_mobile
            , null as address_1
            , null as address_2
            , null as address_3
            , null as address_4
            , null as country
            , add_hash.record_source
            , add_hash.record_load_dts
            , add_hash.hash_diff
    from add_hash

	left outer join [vault].[raw].[sat_customer] ON (
						sat_customer.customer_hash = add_hash.hash_key
					and sat_customer.record_load_end_dts IS NULL
					and sat_customer.record_source = 'PIANO'
					)
	where	add_hash.hash_diff != coalesce(sat_customer.hash_diff, '')
    ;

 	update [vault].[raw].[sat_customer]
 	set record_load_end_dts = sat_max.record_load_dts
 	from [vault].[raw].[sat_customer] sat
 	inner join
 	(
 		select customer_hash,max(record_load_dts) as record_load_dts
 		from [vault].[raw].[sat_customer]
 		where record_source = 'PIANO'
 		group by customer_hash
 	) as sat_max
 	on sat.customer_hash = sat_max.customer_hash
 	and sat.record_load_end_dts is null
 	and sat.record_load_dts < sat_max.record_load_dts
 	and sat.record_source = 'PIANO'
    ;


    -- hub_product
    with products as (
        SELECT
            distinct term__term_id
            , term__aid
            -- , term__name
            , piano__app
            , upper(record_source) as record_source
        FROM [stage].[piano].[piano__subscriptions]
    )

    , hash_data as (
        select *
            , CAST( LOWER(
                CONVERT(NVARCHAR(32),
                HASHBYTES('MD5',
                CONVERT(VARBINARY(MAX),
                CONVERT(VARCHAR(MAX), term__term_id +
                term__aid +
                piano__app +
                record_source
                )
                COLLATE SQL_Latin1_General_CP1_CS_AS))
                , 2)
            ) AS CHAR(32)) AS hash_key
        from products
    )

 	insert into [vault].[raw].[hub_product] (product_hash,product_id,record_source,record_load_dts)
    select hash_key as product_hash
        , term__term_id as product_id
        , record_source as record_source
        , getdate() as record_load_dts
    from hash_data
    where not exists (select product_hash from [vault].[raw].[hub_product] where hub_product.product_hash = hash_data.hash_key and hub_product.record_source = 'PIANO')
    ;

  -- sat_product
    with products as (
        SELECT
            distinct term__term_id
            , term__aid
            , LEFT(term__name, 32) as term__name
            , piano__app
            , upper(record_source) as record_source
        FROM [stage].[piano].[piano__subscriptions]
    )

    , hash_data as (
        select *
            , CAST( LOWER(
                CONVERT(NVARCHAR(32),
                HASHBYTES('MD5',
                CONVERT(VARBINARY(MAX),
                CONVERT(VARCHAR(MAX), term__term_id +
                term__aid +
                piano__app +
                record_source
                )
                COLLATE SQL_Latin1_General_CP1_CS_AS))
                , 2)
            ) AS CHAR(32)) AS hash_key
            , CAST( LOWER(
                CONVERT(NVARCHAR(32),
                HASHBYTES('MD5',
                CONVERT(VARBINARY(MAX),
                CONVERT(VARCHAR(MAX), term__term_id +
                term__aid +
                term__name +
                piano__app +
                record_source
                )
                COLLATE SQL_Latin1_General_CP1_CS_AS))
                , 2)
            ) AS CHAR(32)) AS hash_diff
        from products
    )

 	insert into [vault].[raw].[sat_product]
 			   (
                  product_hash
                , record_load_dts
                , product_id
                , [description]
                , [type]
                , record_source
                , hash_diff
 			   )

    select
        hash_key as product_hash
        , getdate() as record_load_dts
        , term__term_id as product_id
        , piano__app as description
        , term__name as type
        , hash_data.record_source as record_source
        , hash_data.hash_diff as hash_diff
    from hash_data
 	left outer join [vault].[raw].[sat_product] ON (
 		sat_product.product_hash = hash_data.hash_key
 		and sat_product.record_load_end_dts IS NULL
 		and sat_product.record_source = 'PIANO')
 	where hash_data.hash_diff != coalesce(sat_product.hash_diff, '')
    ;

 	update [vault].[raw].[sat_product]
 	set record_load_end_dts = sat_max.record_load_dts
 	from [vault].[raw].[sat_product] sat
 	inner join
 	(
 		select product_hash,max(record_load_dts) as record_load_dts
 		from [vault].[raw].[sat_product]
 		where record_source = 'PIANO'
 		group by product_hash
 	) as sat_max
 	on sat.product_hash = sat_max.product_hash
 	and sat.record_load_end_dts is null
 	and sat.record_load_dts < sat_max.record_load_dts
 	and sat.record_source = 'PIANO'
    ;

-- sat_subscription

    with rawdata as (
        SELECT
            sub__subscription_id as subscription_id -- may have to be null because existing satellite is bigint
            , null as type
            , CONVERT(VARCHAR, sub__start_date_nzt, 23) as start_dt
            , CONVERT(VARCHAR, sub__end_date_nzt, 23) as end_dt
            , null as renewal_dt
            , sub__status as status
            , left(piano__app,12) as campaign
            , upper(record_source) as record_source
            , record_load_dts
            , null as record_load_end_dts
        FROM [stage].[piano].[piano__subscriptions]
    )

    , add_hash as (
        select *
        , CAST( LOWER(
                CONVERT(NVARCHAR(32),
                HASHBYTES('MD5',
                CONVERT(VARBINARY(MAX),
                CONVERT(VARCHAR(MAX), subscription_id +
                campaign +
                record_source
                )
                COLLATE SQL_Latin1_General_CP1_CS_AS))
                , 2)
            ) AS CHAR(32)) AS subscription_hash
        , CAST( LOWER(
            CONVERT(NVARCHAR(32),
            HASHBYTES('MD5',
            CONVERT(VARBINARY(MAX),
            CONVERT(VARCHAR(MAX),
            ISNULL(CONVERT(VARCHAR(MAX), subscription_id), '') +
            ISNULL(rawdata.type, '') +
            ISNULL(CONVERT(VARCHAR, start_dt, 23), '') +
            ISNULL(CONVERT(VARCHAR, end_dt, 23), '') +
            ISNULL(CONVERT(VARCHAR, renewal_dt, 23), '') +
            ISNULL(rawdata.status, '') +
            ISNULL(campaign, '') +
            ISNULL(record_source, '')
            )
            COLLATE SQL_Latin1_General_CP1_CS_AS))
            , 2)
        ) AS CHAR(32)) AS hash_diff
        from rawdata
    )

 	insert into [vault].[raw].[sat_subscription] (
 				 subscription_hash
 				,subscription_id
 				,[type]
 				,start_dt
 				,end_dt
 				,renewal_dt
                ,[status]
 				,campaign
 				,record_source
 				,hash_diff
 			   )

    select
        add_hash.subscription_hash
        , 911 as subscription_id
        , add_hash.type
        , add_hash.start_dt
        , add_hash.end_dt
        , add_hash.renewal_dt
        , add_hash.status
        , add_hash.campaign
        , add_hash.record_source
        , add_hash.hash_diff
    from add_hash
 	left outer join [vault].[raw].[sat_subscription] ON (
 			sat_subscription.subscription_hash = add_hash.subscription_hash
 		and sat_subscription.record_load_end_dts IS NULL
 		and sat_subscription.record_source = 'PIANO')
 	where add_hash.hash_diff != coalesce(sat_subscription.hash_diff, '')
    ;

 	update [vault].[raw].[sat_subscription]
 	set record_load_end_dts = sat_max.record_load_dts
 	from [vault].[raw].[sat_subscription] sat
 	inner join
 	(
 		select subscription_hash,max(record_load_dts) as record_load_dts
 		from [vault].[raw].[sat_subscription]
 		where record_source = 'PIANO'
 	group by subscription_hash
 	) as sat_max
 	on sat.subscription_hash = sat_max.subscription_hash
 	and sat.record_load_end_dts is null
 	and sat.record_load_dts < sat_max.record_load_dts
 	and record_source = 'PIANO'
    ;

-- sat cancellation

    with cancellations as (
        SELECT
            sub__subscription_id as subscription_id -- remove from final
            , piano__app as campaign -- remove from final
            , CONVERT(varchar, DATEADD(second, sub__end_date, '1970-01-01'), 23) as cancellation_dt
            , null as request_dt
            , null as sys_person_id
            , null as source_id
            , upper(record_source) as record_source
            , record_load_dts
            , null as record_load_end_dts
        FROM [stage].[piano].[piano__subscriptions]
        where sub__status in ('cancelled')
    )

    , add_hash as (
        select *
            , CAST( LOWER(
                    CONVERT(NVARCHAR(32),
                    HASHBYTES('MD5',
                    CONVERT(VARBINARY(MAX),
                    CONVERT(VARCHAR(MAX), subscription_id +
                    campaign +
                    record_source
                    )
                    COLLATE SQL_Latin1_General_CP1_CS_AS))
                    , 2)
                ) AS CHAR(32)) AS cancellation_hash
            , CAST( LOWER(
                CONVERT(NVARCHAR(32),
                HASHBYTES('MD5',
                CONVERT(VARBINARY(MAX),
                CONVERT(VARCHAR(MAX),
                ISNULL(CONVERT(VARCHAR(MAX), subscription_id), '') +
                ISNULL(CONVERT(VARCHAR, cancellation_dt, 23), '') +
                ISNULL(campaign, '') +
                ISNULL(record_source, '')
                )
                COLLATE SQL_Latin1_General_CP1_CS_AS))
                , 2)
            ) AS CHAR(32)) AS hash_diff
        from cancellations
    )

	insert into [vault].[raw].[sat_cancellation] (
		  cancellation_hash
		, cancellation_dt
		, request_dts
		, sys_person_id
		, source_id
		, record_source
		, hash_diff
	)

    select add_hash.cancellation_hash
        , add_hash.cancellation_dt
        , add_hash.request_dt
        , add_hash.sys_person_id
        , add_hash.source_id
        , add_hash.record_source
        , add_hash.hash_diff
    from add_hash
	left outer join [vault].[raw].[sat_cancellation] ON (
			sat_cancellation.cancellation_hash = add_hash.cancellation_hash
		and sat_cancellation.record_load_end_dts IS NULL
		and sat_cancellation.record_source = 'PIANO'
        )
	where add_hash.hash_diff != coalesce(sat_cancellation.hash_diff, '')
    ;

	update [vault].[raw].[sat_cancellation]
	set record_load_end_dts = sat_max.record_load_dts
	from [vault].[raw].[sat_cancellation] sat
	inner join
	(
		select cancellation_hash,max(record_load_dts) as record_load_dts
		from [vault].[raw].[sat_cancellation]
		where record_source = 'PIANO'
		group by cancellation_hash
	) as sat_max
	on sat.cancellation_hash = sat_max.cancellation_hash
	and sat.record_load_end_dts is null
	and sat.record_load_dts < sat_max.record_load_dts
	and record_source = 'PIANO'
    ;

    -- link tables
    -- link_subscription

    with links as (
        select
        distinct CAST( LOWER(
                CONVERT(NVARCHAR(32),
                HASHBYTES('MD5',
                CONVERT(VARBINARY(MAX),
                CONVERT(VARCHAR(MAX), sub__subscription_id +
                left(piano__app,12) +
                record_source
                )
                COLLATE SQL_Latin1_General_CP1_CS_AS))
                , 2)
            ) AS CHAR(32)) AS subscription_hash
            , CAST( LOWER(
                    CONVERT(NVARCHAR(32),
                    HASHBYTES('MD5',
                    CONVERT(VARBINARY(MAX),
                    CONVERT(VARCHAR(MAX), record_source  + '|' + user__uid + '|' + user__email )
                    COLLATE SQL_Latin1_General_CP1_CS_AS))
                    , 2)
                ) AS CHAR(32)) AS customer_hash
            , CAST( LOWER(
                    CONVERT(NVARCHAR(32),
                    HASHBYTES('MD5',
                    CONVERT(VARBINARY(MAX),
                    CONVERT(VARCHAR(MAX), term__term_id +
                    term__aid +
                    piano__app +
                    record_source
                    )
                    COLLATE SQL_Latin1_General_CP1_CS_AS))
                    , 2)
                ) AS CHAR(32)) AS product_hash
            , UPPER(record_source) as record_source
            , record_load_dts
        from [stage].[piano].[piano__subscriptions]
    )

 	insert into [vault].[raw].[link_subscription] (
          subscription_id
 		, subscription_hash
 		, customer_hash
 		, product_hash
 		, record_source
 		, record_load_dts
 	)

    select
        123 as subscription_id
        , subscription_hash
 		, customer_hash
 		, product_hash
 		, record_source
 		, record_load_dts from links
 	where not exists (
        select subscription_hash from [vault].[raw].[link_subscription]
        where link_subscription.subscription_hash = links.subscription_hash
        and link_subscription.record_source = 'PIANO'
        )
    ;

    -- link_cancellation

    with links as (
        select
        distinct CAST( LOWER(
                    CONVERT(NVARCHAR(32),
                    HASHBYTES('MD5',
                    CONVERT(VARBINARY(MAX),
                    CONVERT(VARCHAR(MAX), sub__subscription_id +
                    piano__app +
                    record_source
                    )
                    COLLATE SQL_Latin1_General_CP1_CS_AS))
                    , 2)
                ) AS CHAR(32)) AS cancellation_hash
            , CAST( LOWER(
                CONVERT(NVARCHAR(32),
                HASHBYTES('MD5',
                CONVERT(VARBINARY(MAX),
                CONVERT(VARCHAR(MAX), sub__subscription_id +
                left(piano__app,12) +
                record_source
                )
                COLLATE SQL_Latin1_General_CP1_CS_AS))
                , 2)
            ) AS CHAR(32)) AS subscription_hash
            , CAST( LOWER(
                    CONVERT(NVARCHAR(32),
                    HASHBYTES('MD5',
                    CONVERT(VARBINARY(MAX),
                    CONVERT(VARCHAR(MAX), record_source  + '|' + user__uid + '|' + user__email )
                    COLLATE SQL_Latin1_General_CP1_CS_AS))
                    , 2)
                ) AS CHAR(32)) AS customer_hash
            , CAST( LOWER(
                    CONVERT(NVARCHAR(32),
                    HASHBYTES('MD5',
                    CONVERT(VARBINARY(MAX),
                    CONVERT(VARCHAR(MAX), term__term_id +
                    term__aid +
                    piano__app +
                    record_source
                    )
                    COLLATE SQL_Latin1_General_CP1_CS_AS))
                    , 2)
                ) AS CHAR(32)) AS product_hash
            , UPPER(record_source) as record_source
            , record_load_dts
        from [stage].[piano].[piano__subscriptions]
        where sub__status in ('cancelled')
    )

	insert into [vault].[raw].[link_cancellation]
	(
		  cancellation_hash
        , cancellation_reason_hash
		, customer_hash
		, product_hash
		, record_source
		, record_load_dts
	)

    select
          cancellation_hash
        , 123 as cancellation_reason_hash
 		, customer_hash
 		, product_hash
 		, record_source
 		, record_load_dts from links
 	where not exists (
         select cancellation_hash from [vault].[raw].[link_cancellation]
         where link_cancellation.cancellation_hash = links.cancellation_hash
         and link_cancellation.record_source = 'PIANO'
         )
    ;

	insert into [stage].[log].[etl_job] ([user],[source],[job],[status]) values (CURRENT_USER,OBJECT_SCHEMA_NAME(@@PROCID),OBJECT_NAME(@@PROCID),'finished');
GO
