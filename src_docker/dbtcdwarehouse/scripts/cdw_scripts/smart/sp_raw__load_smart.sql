SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [raw].[load_smart] AS
	BEGIN
	-- Usage
	-- exec [smart].[raw].[load_smart];

	-- logging the store procedure activities
	insert into [stage].[log].[etl_job] ([user],[source],[job],[status]) values (CURRENT_USER,OBJECT_SCHEMA_NAME(@@PROCID),OBJECT_NAME(@@PROCID),'started');

	-- Remove the existing dimension and fact tables
	drop table if exists [smart].[raw].[fact_sentiment];
	drop table if exists [smart].[raw].[dim_region];
	drop table if exists [smart].[raw].[dim_sentiment];
	drop table if exists [smart].[raw].[dim_sentiment_why];
	drop table if exists [smart].[raw].[dim_article];
	drop table if exists [smart].[raw].[dim_tag];

	-- Rebuilt all the dimension tables
	create table [smart].[raw].[dim_region](
      region_key INT IDENTITY(1,1) PRIMARY KEY
	, region_name NVARCHAR(100)
	, record_source varchar(50)
    );

	INSERT INTO [smart].[raw].[dim_region](region_name,record_source)
    SELECT '(not set)' as region, '(not set)' as record_source -- id 1
	UNION ALL
	SELECT
	  distinct cast(lower(coalesce(region,'')) as varchar) region
	, record_source
	FROM [vault].[raw].[sat_response]
	  where record_load_end_dts is null
	;

	create table [smart].[raw].[dim_sentiment](
      sentiment_key INT IDENTITY(1,1) PRIMARY KEY
	  , sentiment VARCHAR(50)
	  , record_source varchar(50)
    );

	INSERT INTO [smart].[raw].[dim_sentiment](sentiment,record_source)
	SELECT '(not set)' as answer, '(not set)' as record_source
	UNION ALL
	SELECT
	  distinct lower(coalesce(answer,'')) answer
	  , record_source
	FROM vault.raw.sat_question_response
	  WHERE surveyquestion_id = 'SV_cNNywfC9H5SPVk2QID1' -- id of sentiment question happy or sad?
	  AND record_load_end_dts is null
	;

	create table [smart].[raw].[dim_sentiment_why](
      sentimentwhy_key INT IDENTITY(1,1) PRIMARY KEY
      , question_response_hash CHAR(32)
	  , answer VARCHAR(MAX)
    );

	INSERT INTO [smart].[raw].[dim_sentiment_why](question_response_hash,answer)
	SELECT '(not set)' as question_response_hash, '(not set)' as answer
	UNION ALL
	SELECT question_response_hash
    , case when Answer is null then '(not set)' else lower(coalesce(answer,'')) end as answer
	FROM vault.raw.sat_question_response
	  WHERE surveyquestion_id = 'SV_cNNywfC9H5SPVk2QID12' -- id of sentiment question why form
	  AND record_load_end_dts is null
	;

	create table [smart].[raw].[dim_article](
      article_key INT IDENTITY(1,1) PRIMARY KEY
	  , article_id bigint
	  , title nvarchar(max) NULL
	  ,	author nvarchar(max) NULL
	  ,	published_dts datetime NULL
	  , record_source varchar(50)
    );

	INSERT INTO [smart].[raw].[dim_article](article_id,title,author,published_dts,record_source)
    SELECT -1 as article_id
	, '(not set)' as title
	, '(not set)' as author
	, '1970-01-01 00:00:00.000' as published_dts
	, '(not set)' as record_source
	UNION ALL
	select
	  distinct article_id
	  , title
	  , author
	  , published_dts
	  , record_source
	from vault.raw.sat_article
	where record_load_end_dts is null
	;

	create table [smart].[raw].[dim_tag](
      tag_key INT IDENTITY(1,1) PRIMARY KEY
	  , tag_hash varchar(64)
	  -- , tag_id NVARCHAR(max)
	  , tag_class varchar(50)
	  , tag_value nvarchar(255)
	  , record_source varchar(50)
    );

	-- SOFT BUSINESS RULES TO BE APPLIED HERE!

    WITH tags_data as (
		SELECT '(not set)' as tag_hash
		, '(not set)' as tag_class
		, '(not set)' as tag_value
		, '(not set)' as record_source
		UNION ALL
		SELECT
		t.tag_hash
		, case
			when t.tag_classifier like '%low%' then 'low_level_tags'
			when t.tag_classifier like '%high%' then 'high_level_tags'
			when t.tag_classifier like '%categ%' then 'site_tags'
			else 'site_tags'
			end as tag_class
		, CASE
			WHEN CHARINDEX(':', j.value, CHARINDEX(':', j.value) + 1) > 0 THEN
			LOWER(
				TRIM(
				REPLACE(
				RIGHT(j.value, LEN(j.value) - CHARINDEX(':', j.value, CHARINDEX(':', j.value) + 1)),'''',''
				)))
			WHEN CHARINDEX(':', j.value) > 0 THEN
			LOWER(
				TRIM(
				REPLACE(
				RIGHT(j.value, LEN(j.value) - CHARINDEX(':', j.value)),'''',''
				)))
			ELSE LOWER(TRIM(REPLACE(j.value,'''','')))
			END AS tag_value
		, t.record_source
		FROM (
			SELECT
				tag_id
			, tag_hash
			, record_source
			, tag_classifier
			FROM vault.raw.sat_tag
			where record_load_end_dts is null
		) t
		CROSS APPLY STRING_SPLIT(REPLACE(REPLACE(t.tag_id, '[', ''), ']', ''), ',') j
	),

	tag_hash as (
		SELECT
		CAST( LOWER(
				CONVERT(NVARCHAR(32),
				HASHBYTES('MD5',
				CONVERT(VARBINARY(MAX),
				CONVERT(VARCHAR(MAX), tags_data.record_source  + '|' + tags_data.tag_class + '|' + tags_data.tag_value )
				COLLATE SQL_Latin1_General_CP1_CS_AS))
				, 2)
			) AS CHAR(32)) AS tag_value_hash
	    , tags_data.tag_class
		, tags_data.tag_value
		, tags_data.record_source
		from tags_data
	),

	tags_grouped as (
		select
		tag_hash.tag_value_hash
		, tag_hash.record_source
		, tag_hash.tag_class
		, max(tag_hash.tag_value) as tag_value
		 from tag_hash
		group by tag_hash.record_source, tag_hash.tag_class, tag_hash.tag_value_hash
	)

	INSERT INTO [smart].[raw].[dim_tag](tag_hash,tag_class,tag_value,record_source)
	SELECT * FROM tags_grouped

    -- possible improvement on above query: add index
    -- CREATE NONCLUSTERED INDEX idx_sat_tag_record_load_end_dts ON vault.raw.sat_tag (record_load_end_dts);
	;

	-- Rebuilt the FACT tables

	WITH sentiment_data_cte as (
			select
			sr.response_id
			, cast(sr.recorded_date as datetime) recorded_date
			, cast(lower(coalesce(region,'')) as varchar) as region
			, lower(sqr.Answer) answer
			, case when sqr.surveyquestion_id = 'SV_cNNywfC9H5SPVk2QID1' then ls.question_response_hash else NULL end as question_response_hash
			, sa.article_id
			, st.tag_hash
			from vault.raw.link_surveyquestionresponse ls
			inner join vault.raw.sat_question_response sqr -- sentiment
			on (ls.question_response_hash = sqr.question_response_hash and sqr.surveyquestion_id = 'SV_cNNywfC9H5SPVk2QID1' and sqr.record_load_end_dts is null)
			left join vault.raw.sat_response sr
			on (sr.response_hash = ls.response_hash and sr.record_load_end_dts is null)
			left join vault.raw.sat_article sa
			on (sa.article_hash = ls.article_hash and sa.record_load_end_dts is null)
			left join vault.raw.link_article_tag lat
			on (lat.article_hash = ls.article_hash)
			left join vault.raw.sat_tag st
			on (st.tag_hash = lat.tag_hash and st.record_load_end_dts is null)
			where ls.record_source = 'QUALTRICS'
		),

		sentiment_why as (
			select
			sr.response_id
			, case when sqr.surveyquestion_id = 'SV_cNNywfC9H5SPVk2QID12' then ls.question_response_hash else NULL end as question_response_why_hash
			from vault.raw.link_surveyquestionresponse ls
			inner join vault.raw.sat_question_response sqr -- sentiment why
			on (ls.question_response_hash = sqr.question_response_hash and sqr.surveyquestion_id = 'SV_cNNywfC9H5SPVk2QID12' and sqr.record_load_end_dts is null)
			left join vault.raw.sat_response sr
			on (sr.response_hash = ls.response_hash and sr.record_load_end_dts is null)
			where ls.record_source = 'QUALTRICS'
		),

		merge_data as (
			select c.*, w.question_response_why_hash from sentiment_data_cte c
			left join sentiment_why w on w.response_id = c.response_id
		),

		tags_data as (

		SELECT '(not set)' as tag_hash
		, '(not set)' as tag_class
		, '(not set)' as tag_value
		, '(not set)' as record_source

		UNION ALL
		SELECT
		t.tag_hash
		, case
			when t.tag_classifier like '%low%' then 'low_level_tags'
			when t.tag_classifier like '%high%' then 'high_level_tags'
			when t.tag_classifier like '%categ%' then 'site_tags'
			else 'site_tags'
			end as tag_class
		, CASE
			WHEN CHARINDEX(':', j.value, CHARINDEX(':', j.value) + 1) > 0 THEN
			LOWER(
				TRIM(
				REPLACE(
				RIGHT(j.value, LEN(j.value) - CHARINDEX(':', j.value, CHARINDEX(':', j.value) + 1)),'''',''
				)))
			WHEN CHARINDEX(':', j.value) > 0 THEN
			LOWER(
				TRIM(
				REPLACE(
				RIGHT(j.value, LEN(j.value) - CHARINDEX(':', j.value)),'''',''
				)))
			ELSE LOWER(TRIM(REPLACE(j.value,'''','')))
			END AS tag_value
		, t.record_source
		FROM (
			SELECT
			tag_id
			, tag_hash
			, record_source
			, tag_classifier
			FROM vault.raw.sat_tag
			where record_load_end_dts is null
		) t
		CROSS APPLY STRING_SPLIT(REPLACE(REPLACE(t.tag_id, '[', ''), ']', ''), ',') j
		),

		tag_hash as (
			SELECT
			CAST( LOWER(
					CONVERT(NVARCHAR(32),
					HASHBYTES('MD5',
					CONVERT(VARBINARY(MAX),
					CONVERT(VARCHAR(MAX), tags_data.record_source  + '|' + tags_data.tag_class + '|' + tags_data.tag_value )
					COLLATE SQL_Latin1_General_CP1_CS_AS))
					, 2)
				) AS CHAR(32)) AS tag_value_hash
			, tags_data.tag_hash
			from tags_data
		),

		merge_tag as (
			select m.*
			, t.tag_value_hash
			from merge_data m
			left join tag_hash t on t.tag_hash = m.tag_hash
		)

		select
			coalesce(da.article_key, 1) as article_key
			, sd.response_id
			, coalesce(ds.sentiment_key, 1) as sentiment_key
			, coalesce(dw.sentimentwhy_key,1) as sentimentwhy_key
			, coalesce(dr.region_key, 1) as region_key
			, coalesce(dt.tag_key, 1) as tag_key
            , sd.recorded_date
		into [smart].[raw].[fact_sentiment]
		from merge_tag sd
		left join smart.raw.dim_region dr
		on (sd.region = dr.region_name)
		left join smart.raw.dim_sentiment ds
		on (sd.answer = ds.sentiment)
		left join smart.raw.dim_sentiment_why dw
		on (sd.question_response_why_hash = dw.question_response_hash)
		left join smart.raw.dim_article da
		on (sd.article_id = da.article_id)
		left join smart.raw.dim_tag dt
		on (sd.tag_value_hash = dt.tag_hash)
	;

	-- Rebuilt all the integrity constraints
	ALTER TABLE [raw].[fact_sentiment]  WITH CHECK ADD  CONSTRAINT [FK_fact_sentiment_dim_article] FOREIGN KEY([article_key])
	REFERENCES [raw].[dim_article] ([article_key]);

	ALTER TABLE [raw].[fact_sentiment]  WITH CHECK ADD  CONSTRAINT [FK_fact_sentiment_dim_region] FOREIGN KEY([region_key])
	REFERENCES [raw].[dim_region] ([region_key]);

	ALTER TABLE [raw].[fact_sentiment]  WITH CHECK ADD  CONSTRAINT [FK_fact_sentiment_dim_sentiment] FOREIGN KEY([sentiment_key])
	REFERENCES [raw].[dim_sentiment] ([sentiment_key]);

	ALTER TABLE [raw].[fact_sentiment]  WITH CHECK ADD  CONSTRAINT [FK_fact_sentiment_dim_sentiment_why] FOREIGN KEY([sentimentwhy_key])
	REFERENCES [raw].[dim_sentiment_why] ([sentimentwhy_key]);

	ALTER TABLE [raw].[fact_sentiment]  WITH CHECK ADD  CONSTRAINT [FK_fact_sentiment_dim_tag] FOREIGN KEY([tag_key])
	REFERENCES [raw].[dim_tag] ([tag_key]);

	-- Rebuilt indexes
	create unique index ix_dim_region_record_source_region_id on [smart].[raw].[dim_region] (record_source,region_name);
	create unique index ix_dim_sentiment_record_source_sentiment_id on [smart].[raw].[dim_sentiment] (record_source,sentiment);
	create unique index ix_dim_sentiment_why_record_source_why_id on [smart].[raw].[dim_sentiment_why] (question_response_hash);
	create unique index ix_dim_article_record_source_article_id on [smart].[raw].[dim_article] (record_source,article_id);
	create unique index ix_dim_tag_record_source_tag_id on [smart].[raw].[dim_tag] (record_source,tag_hash,tag_value);

	insert into [stage].[log].[etl_job] ([user],[source],[job],[status]) values (CURRENT_USER,OBJECT_SCHEMA_NAME(@@PROCID),OBJECT_NAME(@@PROCID),'finished');

	END
GO
