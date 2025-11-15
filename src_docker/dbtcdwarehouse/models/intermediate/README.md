follow dbt best practise: https://docs.getdbt.com/guides/best-practices/how-we-structure/3-intermediate

vault:

    vault.raw.hub_article <-- composer only (this)
    vault.raw.sat_article <-- composer only (this)
    vault.raw.sat_article_tag <-- composer + parsely (this is new)

    vault.raw.link_article_question_response <-- this is new

    vault.raw.hub_tag <-- composer & parsely (gone)
    vault.raw.sat_tag <-- composer & parsely (gone)

    vault.raw.hub_question_response <-- qualtrics only
    vault.raw.sat_question_response <-- qualtrics only
    vault.raw.sat_response <-- qualtrics only (this is new merge with above?)

    vault.raw.hub_response <-- qualtrics only  (gone)
    vault.raw.sat_response <-- qualtrics only (gone)

    vault.raw.link_article_tag (gone)
    vault.raw.link_surveyquestionresponse ? -- what is this made of (gone)

sources:

  link_article_tag
    1. composer.tags linked to composer.composer__articles
    2. parsely.tags linked to composer.composer__articles

  link_surveyquestionresponse
    [stage].[QUALTRICS].[surveyquestion]
    [stage].[QUALTRICS].[surveyquestionresponses]
    [stage].[QUALTRICS].[surveyresponses]
    [stage].[composer].[composer__articles]
    [stage].[QUALTRICS].[articles]
    -- members -- ignore for now

  composer.tags -> hub_tag

  parsely.tags ->  hub_tag

 [QUALTRICS].[surveyquestion] -> hub_question

  QUALTRICS.surveyresponses -> hub_response (response dimensions)
    <!  -- hash_key
		  ,responseid
		  ,record_source
		  ,record_load_dts -->

  QUALTRICS.surveyquestionresponses -> hub_question_response
		   hash_key (surveyid, ResponseId, Questionid)
		  ,concat (surveyid, ResponseId, Questionid) -- naturalkey
		  ,record_source
		  ,record_load_dts

  composer.articles -> hub_article (this should be from composer.composer__articles)
    --     hash_key -- (record_source, content_id)
    --   , a.content_id -- natural key / primary key
    --   , a.record_source
    --   , a.record_load_dts_utc as record_load_dts

  composer.articles -> sat_article (this should be from composer.composer__articles)
    -- hash_diff
    --    TRY_CONVERT(int, [content_id]) as content_id
    --     , CAST([hash_key] AS CHAR(32)) as hash_key
    --     , [title]
    --     , [author]
    --     , [published_dts]
    --     , [source]
    --     , [brand]
    --     , [category]
    --     , [category_1]
    --     , [category_2]
    --     , [category_3]
    --     , [category_4]
    --     , [category_5]
    --     , [category_6]
    --     , [word_count]
    --     , [print_slug]
    --     , [advertisement]
    --     , [sponsored]
    --     , [promoted_flag]
    --     , [comments_flag]
    --     , [home_flag]
    --     , [record_source]
    --     , record_load_dts
