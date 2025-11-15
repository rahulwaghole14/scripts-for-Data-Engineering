SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE   PROCEDURE [raw].[load_imart] AS

	-- Usage
	-- exec [imart].[raw].[load_imart];

	insert into [stage].[log].[etl_job] ([user],[source],[job],[status]) values (CURRENT_USER,OBJECT_SCHEMA_NAME(@@PROCID),OBJECT_NAME(@@PROCID),'started');

	-- SOFT BUSINESS RULES TO BE APPLIED HERE!

	drop table if exists [imart].[raw].[dim_product];

		select distinct product_hash,product_id,[description],[type],price,record_source
		into [imart].[raw].[dim_product]
		from [vault].[raw].[sat_product]
		where record_load_end_dts is null
	;

	drop table if exists [imart].[raw].[dim_customer];

		select distinct customer_hash,customer_id,[type]
		,case
			when record_source = 'MATRIX' then null
			when record_source = 'FIBRE' and [status] = 'ACTIVE' then 1 else 0
		end as [active]
		,[status],title,first_name,middle_name,last_name,dob,phone_home,phone_mobile,phone_work,email,email_alternative,address_type,address_1,address_2,address_3,address_4
		,case
			when record_source <> 'IDM' then substring([address_4],patindex('%[0-9]%', [address_4]),4)
			else null
		end as [postcode]
		,dpid,country,latitude,longitude
		,record_source
		into [imart].[raw].[dim_customer]
		from [vault].[raw].[sat_customer]
		where record_load_end_dts is null
	;

	drop table if exists [imart].[raw].[dim_payment];

		select distinct payment_hash,payment_id,[type],amount,[description],payment_dt,record_source
		into [imart].[raw].[dim_payment]
		from [vault].[raw].[sat_payment]
		where record_load_end_dts is null
	;

	drop table if exists [imart].[raw].[fact_subscription];

		select distinct
		 link_subscription.record_source
		,link_subscription.subscription_id
		,sat_customer.customer_id as customer_id
		,sat_product.product_id as product_id
		,sat_payment.payment_id as payment_id
		,sat_subscription.start_dt
		,sat_subscription.end_dt
		,sat_subscription.[status]

		into [imart].[raw].[fact_subscription]
		-- select count(1)
		from [vault].[raw].[link_subscription]
			inner join [vault].[raw].[sat_customer] on link_subscription.customer_hash = sat_customer.customer_hash and sat_customer.record_load_end_dts is null
			inner join [vault].[raw].[sat_product] on link_subscription.product_hash = sat_product.product_hash and sat_product.record_load_end_dts is null
			left join [vault].[raw].[sat_payment] on link_subscription.payment_hash = sat_payment.payment_hash and sat_payment.record_load_end_dts is null
			left join [vault].[raw].[sat_subscription] on link_subscription.subscription_hash = sat_subscription.subscription_hash and sat_subscription.record_load_end_dts is null
		where [vault].[raw].[sat_customer].[record_source] !='PRESSPATRON' -- exclude alpha-numeric value
	;


/*  Retired at
	drop table if exists [raw].[fibre_all];

		select
		 dim_customer.*
		,dim_product.product_id
		,dim_product.[description]
		,dim_product.[type] as product_type
		,dim_product.price

		,dim_payment.payment_id
		,dim_payment.[type] as payment_type
		,dim_payment.amount
		,dim_payment.[description] as payment_description
		,dim_payment.payment_dt

		,fact_subscription.subscription_id
		,fact_subscription.[status] as subscription_status

		into [imart].[raw].[fibre_all]
		-- select count(1)
		FROM [imart].[raw].fact_subscription
			left join [imart].[raw].dim_customer on dim_customer.customer_id = fact_subscription.customer_id
			left join [imart].[raw].dim_product on dim_product.product_id = fact_subscription.product_id
			left join [imart].[raw].dim_payment on dim_payment.payment_id = fact_subscription.payment_id

		where	fact_subscription.record_source = 'FIBRE'
		and		dim_customer.record_source = 'FIBRE'
		and		dim_product.record_source = 'FIBRE'
		and		coalesce(dim_payment.record_source,'') in ('FIBRE','')	--as it is a left join
		;
*/
	alter table [imart].[raw].[dim_customer] add constraint pk_dim_customer primary key nonclustered (customer_hash);
	alter table [imart].[raw].[dim_product] add constraint pk_dim_product primary key nonclustered (product_hash);
	alter table [imart].[raw].[dim_payment] add constraint pk_dim_payment primary key nonclustered (payment_hash);
	/*
	alter table [imart].[raw].[fact_subscription] add constraint fk_fact_subscription_dim_customer foreign key (customer_id) references dim_customer(customer_hash);
	alter table [imart].[raw].[fact_subscription] add constraint fk_fact_subscription_dim_product foreign key (product_id) references dim_product(product_hash);
	alter table [imart].[raw].[fact_subscription] add constraint fk_fact_subscription_dim_payment foreign key (payment_id) references dim_payment(payment_hash);
	*/

	create unique index ix_dim_customer_record_source_customer_id on [imart].[raw].[dim_customer] (record_source,customer_id);
	create unique index ix_dim_product_record_source_product_id on [imart].[raw].[dim_product] (record_source,product_id);
	create unique index ix_dim_payment_record_source_payment_id on [imart].[raw].[dim_payment] (record_source,payment_id,[description]);



	insert into [stage].[log].[etl_job] ([user],[source],[job],[status]) values (CURRENT_USER,OBJECT_SCHEMA_NAME(@@PROCID),OBJECT_NAME(@@PROCID),'finished');


GO
