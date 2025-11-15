SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [composer].[load_vault] AS

	-- Usage
	-- exec [stage].[COMPOSER].[load_vault];
	insert into [stage].[log].[etl_job] ([user],[source],[job],[status]) values (CURRENT_USER,OBJECT_SCHEMA_NAME(@@PROCID),OBJECT_NAME(@@PROCID),'started');

	with articles as (
		SELECT DISTINCT CAST(
        COALESCE( a.hash_key,
				LOWER(
				CONVERT(NVARCHAR(32),
				HASHBYTES('MD5',
				CONVERT(VARBINARY(MAX),
				CONVERT(VARCHAR(MAX), 'COMPOSER' + '|' + CONVERT(NVARCHAR(20), a.content_id))
				COLLATE SQL_Latin1_General_CP1_CS_AS))
				, 2)
			) ) AS CHAR(32)) AS hash_key
      , a.content_id
			, a.record_source
			, a.record_load_dts_utc as record_load_dts
		FROM [stage].[COMPOSER].[composer__articles] AS a
	),

  final as (
    select a.* from articles a
    LEFT JOIN [vault].[raw].[hub_article] AS ha ON ha.article_hash = a.hash_key AND ha.record_source = 'COMPOSER'
		WHERE ha.article_hash IS NULL

  )

  insert into [vault].[raw].[hub_article] (article_hash,article_id,record_source,record_load_dts)
	select * from final
	;
	-- select * from [vault].[raw].[hub_article] where record_source = 'COMPOSER';

  with articles as (
	select
      distinct articles.[content_id]
      , CAST(
        COALESCE( articles.hash_key,
        LOWER(
            CONVERT(NVARCHAR(32),
               HASHBYTES('MD5',
                CONVERT(VARBINARY(MAX),
                CONVERT(VARCHAR(MAX), 'COMPOSER' + '|' + CONVERT(NVARCHAR(20), content_id))
             COLLATE SQL_Latin1_General_CP1_CS_AS))
            , 2)
			  ) ) AS CHAR(32)) AS hash_key
        , articles.[title]
        , articles.[author]
        , articles.[published_dts]
        , articles.[source]
        , articles.[brand]
        , articles.[category]
        , articles.[category_1]
        , articles.[category_2]
        , articles.[category_3]
        , articles.[category_4]
        , articles.[category_5]
        , articles.[category_6]
        , articles.[word_count]
        , articles.[print_slug]
        , articles.[advertisement]
        , articles.[sponsored]
        , articles.[promoted_flag]
        , articles.[comments_flag]
        , articles.[home_flag]
        , articles.[record_source]
        , articles.[record_load_dts_utc] as record_load_dts
        , articles.[hash_diff]
        , concat(
          articles.[category]
          , articles.[content_id]
          , articles.[title]
          , articles.[author]
          , articles.[source]
          , articles.[brand]
          , articles.[category_1]
          , articles.[category_2]
          , articles.[category_3]
          , articles.[category_4]
          , articles.[category_5]
          , articles.[category_6]) as concat_diff
	from [stage].[COMPOSER].[composer__articles] articles
    ),

    hash_diff as (
        SELECT
         TRY_CONVERT(int, [content_id]) as content_id
        , CAST([hash_key] AS CHAR(32)) as hash_key
        , [title]
        , [author]
        , [published_dts]
        , [source]
        , [brand]
        , [category]
        , [category_1]
        , [category_2]
        , [category_3]
        , [category_4]
        , [category_5]
        , [category_6]
        , [word_count]
        , [print_slug]
        , [advertisement]
        , [sponsored]
        , [promoted_flag]
        , [comments_flag]
        , [home_flag]
        , [record_source]
        , record_load_dts
        , CAST(COALESCE( hash_diff, LOWER(
				CONVERT(NVARCHAR(32),
				HASHBYTES('MD5',
				CONVERT(VARBINARY(MAX),
				CONVERT(VARCHAR(MAX), 'COMPOSER' + '|' + CONVERT(NVARCHAR, concat_diff))
				COLLATE SQL_Latin1_General_CP1_CS_AS))
				, 2)
	  		)) AS CHAR(32)) AS hash_diff
        FROM articles
    ),

    raw_sat_article AS (
		SELECT sa.article_hash, sa.hash_diff
		FROM vault.raw.sat_article AS sa
		WHERE sa.record_load_end_dts IS NULL
		AND sa.record_source = 'COMPOSER'
    ),

    final AS (
		SELECT a.*
		FROM hash_diff AS a
		LEFT JOIN raw_sat_article AS rsa ON (rsa.article_hash = a.hash_key)
		WHERE a.hash_diff != COALESCE(rsa.hash_diff, '')
    )

	insert into [vault].[raw].[sat_article]
			   (

      [article_id]
      , [article_hash]
      ,[title]
      ,[author]
      ,[published_dts]
      ,[source]
      ,[brand]
      ,[category]
      ,[category_1]
      ,[category_2]
      ,[category_3]
      ,[category_4]
      ,[category_5]
      ,[category_6]
      ,[word_count]
      ,[print_slug]
      ,[advertisement]
      ,[sponsored]
      ,[promoted_flag]
      ,[comments_flag]
      ,[home_flag]
      ,[record_source]
      ,[record_load_dts]
      ,[hash_diff]
      )

	select * from final
	;
	-- select * from [vault].[raw].[sat_article] where record_source = 'COMPOSER';

	update [vault].[raw].[sat_article]
	set record_load_end_dts = sat_max.record_load_dts
	from [vault].[raw].[sat_article] sat
	inner join
	(
		select article_hash,max(record_load_dts) as record_load_dts
		from [vault].[raw].[sat_article]
		where record_source = 'COMPOSER'
		group by article_hash
	) as sat_max
	on sat.article_hash = sat_max.article_hash
	and sat.record_load_end_dts is null
	and sat.record_load_dts < sat_max.record_load_dts
	and sat.record_source = 'COMPOSER'
	;

   ------------------------------------------------------------------------

    insert into [vault].[raw].[hub_tag] (tag_hash,tag_id,record_source,record_load_dts)
	select distinct
		   hash_key
		  ,category
		  ,record_source
		  ,record_load_dts
	from [stage].[COMPOSER].[tags]
	where not exists (select tag_hash from [vault].[raw].[hub_tag] where hub_tag.tag_hash = tags.hash_key and hub_tag.record_source = 'COMPOSER')
	;
	-- select * from [vault].[raw].[hub_tag] where record_source = 'COMPOSER';

	insert into [vault].[raw].[sat_tag]
			   (tag_hash
               , record_load_dts
               , tag_id
               , tag_classifier
               , tag_value
               , last_modified
               , creation_date
               , record_source
               , hash_diff  )
	select distinct
	  tags.hash_key
      , tags.[record_load_dts]
      , tags.[category]
      , tags.[tagType]
      , tags.[tag]
      , tags.[record_load_dts]
      , tags.[max_published_dts]
      , tags.[record_source]
      , tags.[hash_diff]
	from [stage].[COMPOSER].[tags] tags
		left outer join [vault].[raw].[sat_tag] ON (
						sat_tag.tag_hash = tags.hash_key
					and sat_tag.record_load_end_dts IS NULL
					and sat_tag.record_source = 'COMPOSER'
					)
	where	tags.hash_diff != coalesce(sat_tag.hash_diff, '')
	;
	-- select * from [vault].[raw].[sat_tag] where record_source = 'COMPOSER';

	update [vault].[raw].[sat_tag]
	set record_load_end_dts = sat_max.record_load_dts
	from [vault].[raw].[sat_tag] sat
	inner join
	(
		select tag_hash,max(record_load_dts) as record_load_dts
		from [vault].[raw].[sat_tag]
		where record_source = 'COMPOSER'
		group by tag_hash
	) as sat_max
	on sat.tag_hash = sat_max.tag_hash
	and sat.record_load_end_dts is null
	and sat.record_load_dts < sat_max.record_load_dts
	and sat.record_source = 'COMPOSER'
	;

	------------------------------------------------------------------------

	with tags as (
        SELECT
        t.hash_key as tag_hash
        , t.category as article_tag_id
        , t.record_source
        , t.record_load_dts
        from [stage].[composer].[tags] t
    ),

    articles as (
        select distinct CAST(
        COALESCE( a.hash_key,
            LOWER(
            CONVERT(NVARCHAR(32),
            HASHBYTES('MD5',
            CONVERT(VARBINARY(MAX),
            CONVERT(VARCHAR(MAX), 'COMPOSER' + '|' + CONVERT(NVARCHAR(20), a.content_id))
            COLLATE SQL_Latin1_General_CP1_CS_AS))
            , 2)
        ) ) AS CHAR(32)) AS article_hash
        , a.category
        from [stage].[composer].[composer__articles] a
    ),

    final as (
        select a.article_hash, t.* from tags t
        inner join articles a on a.category = t.article_tag_id
        LEFT JOIN [vault].[raw].[link_article_tag] AS lat
        ON lat.tag_hash = t.tag_hash
        AND lat.record_source = 'COMPOSER'
        WHERE
        lat.tag_hash IS NULL
    )

	insert into [vault].[raw].[link_article_tag] (
		 article_hash
		,tag_hash
		,at_id
		,record_source
		,record_load_dts
	)

	select * from final
	;
	-- 56 services with no customers out of 50,000 (0,1%)

	--  select count(1),count(distinct article_tag_hash) from [vault].[raw].[link_article_tag] where record_source = 'COMPOSER';
	--  select top 1000 * from [vault].[raw].[link_article_tag] where record_source = 'COMPOSER' order by 2;

    	insert into [stage].[log].[etl_job] ([user],[source],[job],[status]) values (CURRENT_USER,OBJECT_SCHEMA_NAME(@@PROCID),OBJECT_NAME(@@PROCID),'finished');
GO
