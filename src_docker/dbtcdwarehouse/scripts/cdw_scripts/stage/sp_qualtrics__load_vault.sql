SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [qualtrics].[load_vault] @component varchar(20) AS

	-- Usage
	-- exec [stage].[qualtrics].[load_vault] @component = N'responses' OR exec [stage].[qualtrics].[load_vault] @component = N'questions';

	insert into [stage].[log].[etl_job] ([user],[source],[job],[status]) values (CURRENT_USER,OBJECT_SCHEMA_NAME(@@PROCID),OBJECT_NAME(@@PROCID),'started');

	/*
	delete from [vault].[raw].[sat_customer] where record_source = 'QUALTRICS';
	delete from [vault].[raw].[hub_customer] where record_source = 'QUALTRICS';
	delete from [vault].[raw].[hub_question] where record_source = 'QUALTRICS';
	delete from [vault].[raw].[sat_question] where record_source = 'QUALTRICS';
	delete from [vault].[raw].[hub_response] where record_source = 'QUALTRICS';
	delete from [vault].[raw].[sat_response] where record_source = 'QUALTRICS';
	delete from [vault].[raw].[hub_question_response] where record_source = 'QUALTRICS';
	delete from [vault].[raw].[sat_question_response] where record_source = 'QUALTRICS';
	delete from [vault].[raw].[sat_survey] where record_source = 'QUALTRICS';
	delete from [vault].[raw].[hub_survey] where record_source = 'QUALTRICS';
	delete from [vault].[raw].[link_survey_question] where record_source = 'QUALTRICS';
	delete from [vault].[raw].[link_surveyquestionresponse] where record_source = 'QUALTRICS';
	delete from [vault].[raw].[sat_question_response] where record_source = 'QUALTRICS';
	*/

if lower(@component) = 'questions' begin

    insert into [vault].[raw].[hub_survey] (survey_hash,survey_id,record_source,record_load_dts)
	select distinct
		   hash_key
		  ,id
		  ,record_source
		  ,record_load_dts
	from [stage].[QUALTRICS].[survey]
	where not exists (select survey_hash from [vault].[raw].[hub_survey] where hub_survey.survey_hash = survey.hash_key and hub_survey.record_source = 'QUALTRICS')
	;

	insert into [vault].[raw].[sat_survey] (
            survey_hash,
            record_load_dts,
            survey_id,
            owner_id,
            survey_name,
            last_modified,
            creation_date,
            is_active,
            record_source,
	        hash_diff
	)
	select distinct
      [survey].[hash_key]
      ,[survey].record_load_dts
      ,[survey].[id]
      ,[survey].[ownerId]
      ,[survey].[name]
      ,[survey].[lastModified]
      ,[survey].[creationDate]
      ,[survey].[isActive]
      ,[survey].[record_source]
      ,[survey].[hash_diff]
	from [stage].[QUALTRICS].[survey]
		left outer join [vault].[raw].[sat_survey] ON (
						sat_survey.survey_hash = survey.hash_key
					and sat_survey.record_load_end_dts IS NULL
					and sat_survey.record_source = 'QUALTRICS'
					)
	where	survey.hash_diff != coalesce(sat_survey.hash_diff, '')
	;
	-- select top 100 * from [vault].[raw].[sat_survey] where record_source = 'QUALTRICS';

	update [vault].[raw].[sat_survey]
	set record_load_end_dts = sat_max.record_load_dts
	from [vault].[raw].[sat_survey] sat
	inner join
	(
		select survey_hash,max(record_load_dts) as record_load_dts
		from [vault].[raw].[sat_survey]
		where record_source = 'QUALTRICS'
		group by survey_hash
	) as sat_max
	on sat.survey_hash = sat_max.survey_hash
	and sat.record_load_end_dts is null
	and sat.record_load_dts < sat_max.record_load_dts
	and sat.record_source = 'QUALTRICS'
	;
-------------------------------------------------------------------------------------------------------------------

	insert into [vault].[raw].[hub_question] (question_hash,survey_id, question_id,record_source,record_load_dts)
	select distinct
		   hash_key
		  ,surveyid
		  ,questionid
		  ,record_source
		  ,record_load_dts
	from [stage].[QUALTRICS].[surveyquestion]
	where not exists (select question_hash from [vault].[raw].[hub_question] where hub_question.question_hash = surveyquestion.hash_key and hub_question.record_source = 'QUALTRICS')
	;
	-- select * from [vault].[raw].[hub_question] where record_source = 'QUALTRICS';

	insert into [vault].[raw].[sat_question]
			   (question_hash, record_load_dts, survey_id, question_id, question_text, default_choices, question_type, question_desc,
	selector, subselector, dataexport_tag, choice_order, record_source, hash_diff
			   )
	select distinct [surveyquestion].[hash_key]
	,[surveyquestion].[record_load_dts]
      ,[surveyquestion].[surveyId]
      ,[surveyquestion].[QuestionID]
      ,[surveyquestion].[QuestionText]
      ,[surveyquestion].[DefaultChoices]
	  ,[surveyquestion].[QuestionType]
      ,[surveyquestion].[QuestionDescription]
      ,[surveyquestion].[Selector]
      ,[surveyquestion].[SubSelector]
      ,[surveyquestion].[DataExportTag]
      ,[surveyquestion].[ChoiceOrder]
	  ,[surveyquestion].[record_source]
      ,[surveyquestion].[hash_diff]
  FROM [stage].[qualtrics].[surveyquestion]
		left outer join [vault].[raw].[sat_question] ON (
						sat_question.question_hash = surveyquestion.hash_key
					and sat_question.record_load_end_dts IS NULL
					and sat_question.record_source = 'QUALTRICS'
					)
	where	surveyquestion.hash_diff != coalesce(sat_question.hash_diff, '')
	;
	-- select * from [vault].[raw].[sat_question] where record_source = 'QUALTRICS';

	update [vault].[raw].[sat_question]
	set record_load_end_dts = sat_max.record_load_dts
	from [vault].[raw].[sat_question] sat
	inner join
	(
		select question_hash,max(record_load_dts) as record_load_dts
		from [vault].[raw].[sat_question]
		where record_source = 'QUALTRICS'
		group by question_hash
	) as sat_max
	on sat.question_hash = sat_max.question_hash
	and sat.record_load_end_dts is null
	and sat.record_load_dts < sat_max.record_load_dts
	and sat.record_source = 'QUALTRICS'
	;

	insert into [vault].[raw].[link_survey_question] (
		survey_hash
		,question_hash
	--	,panel_hash
	--	,period_hash
		,surveyquestion_id
		,record_source
		,record_load_dts
	)
	select distinct
		 [survey].hash_key
		,surveyquestion.hash_key as question_hash
		,[surveyquestion].surveyid+[surveyquestion].questionid as survey_question_id
		,[surveyquestion].record_source
		,[surveyquestion].record_load_dts
	/*
	select distinct top 1000

	*/
	-- select count(1),count(distinct [surveyquestion].hash_key)
	from [stage].[QUALTRICS].[surveyquestion]
	inner join [stage].[QUALTRICS].[survey] on survey.id = [surveyquestion].surveyid

	where not exists (select 1 from [vault].[raw].[link_survey_question]
    where link_survey_question.question_hash = [surveyquestion].hash_key and link_survey_question.record_source = 'QUALTRICS')
	;

end

if lower(@component) = 'responses' begin

--     with members_cte as (
--     select [record_load_dts]
--       ,[sequence_nr]
--       ,[record_source]
--       ,[hash_key]
--       ,[hash_diff]
--       ,[IPAddress]
--       ,[ResponseId]
--       ,[RecipientFirstName]
--       ,[RecipientEmail]
--       ,[LocationLatitude]
--       ,[LocationLongitude]
--       ,[UserLanguage]
--       ,[user_memberid]
--       ,[surveyType] from [stage].[qualtrics].[members]
--       where sequence_nr in (select max(sequence_nr) from [stage].[qualtrics].[members] group by hash_key ))
--
--       select * from members_cte

	insert into [vault].[raw].[hub_customer] (customer_hash,customer_id,record_source,record_load_dts)
	select distinct
		   hash_key
		  ,user_memberid
		  ,record_source
		  ,record_load_dts
	from [stage].[qualtrics].[members]
	where not exists (select customer_hash from [vault].[raw].[hub_customer]
						where hub_customer.customer_hash = members.hash_key and hub_customer.record_source = 'QUALTRICS')
	;

	insert into [vault].[raw].[sat_customer] (
		 customer_hash
		,customer_id
		,[type]
		-- ,active
		,[status]
		,first_name
		-- ,middle_name
		--,last_name
		--,dob
		,email --,email_alternative
		,latitude,longitude
		,record_source,record_load_dts,hash_diff
	)
	select distinct
		 members.hash_key
		,members.user_memberid
		,members.[surveytype]
		,members.[status]
		,members.firstname
		,members.email
		,members.locationlatitude
		,members.locationlongitude
		,members.record_source
		,members.record_load_dts
		,members.hash_diff
	from (
		select distinct member.hash_key
		,member.user_memberid
		,member.[surveytype]
		,case when member.recipientfirstname is null and member.recipientemail is null then
		 'User profile from IDM'
		 ELSE
		 'User profile from Qualtrics'
		end [status]
		,coalesce(member.recipientfirstname,drupal__user_profiles.first_name) firstname
		,coalesce(member.recipientemail,drupal__user_profiles.email) email
		,case when member.user_memberid = 'unknown' then
		 '0.0'
		 ELSE
		 member.locationlatitude
		end locationlatitude
		,case when member.user_memberid = 'unknown' then
		 '0.0'
		 ELSE
		 member.locationlongitude
		end locationlongitude
		,member.record_source
		,member.record_load_dts
		,case when member.user_memberid = 'unknown' then
		 member.hash_key
		 ELSE
		 member.hash_diff
		end hash_diff from (select [record_load_dts]
      ,[sequence_nr]
      ,[record_source]
      ,[hash_key]
      ,[hash_diff]
      ,[IPAddress]
      ,[ResponseId]
      ,[RecipientFirstName]
      ,[RecipientEmail]
      ,[LocationLatitude]
      ,[LocationLongitude]
      ,[UserLanguage]
      ,[user_memberid]
      ,[surveyType] from [stage].[qualtrics].[members]
      where sequence_nr in (select max(sequence_nr) from [stage].[qualtrics].[members] group by hash_key )) member
		left outer join [stage].idm.drupal__user_profiles on (member.user_memberid = cast(drupal__user_profiles.user_id as nvarchar))
	) members
		left outer join [vault].[raw].[sat_customer] ON (
						sat_customer.customer_hash = members.hash_key
					and sat_customer.record_load_end_dts IS NULL
					and sat_customer.record_source = 'QUALTRICS'
					)
	where	members.hash_diff != coalesce(sat_customer.hash_diff, '')
	;

	-- select top 100 * from [vault].[raw].[sat_customer] where record_source = 'QUALTRICS';
	-- select count(1),count(distinct customer_hash) from [vault].[raw].[sat_customer] where record_source = 'QUALTRICS';

	update [vault].[raw].[sat_customer]
	set record_load_end_dts = sat_max.record_load_dts
	from [vault].[raw].[sat_customer] sat
	inner join
	(
		select customer_hash,max(record_load_dts) as record_load_dts
		from [vault].[raw].[sat_customer]
		where record_source = 'QUALTRICS'
		group by customer_hash
	) as sat_max
	on sat.customer_hash = sat_max.customer_hash
	and sat.record_load_end_dts is null
	and sat.record_load_dts < sat_max.record_load_dts
	and sat.record_source = 'QUALTRICS'
	;

	----------------------------------------------------------------------

	insert into [vault].[raw].[hub_response] (response_hash, response_id,record_source,record_load_dts)
	select distinct
		   hash_key
		  ,responseid
		  ,record_source
		  ,record_load_dts
	from [stage].[QUALTRICS].[surveyresponses]
	where not exists (select response_hash from [vault].[raw].[hub_response] where hub_response.response_hash = surveyresponses.hash_key and hub_response.record_source = 'QUALTRICS')
	;
	-- select * from [vault].[raw].[hub_response] where record_source = 'QUALTRICS';

	insert into [vault].[raw].[sat_response]
			   ([response_hash]
      ,[record_load_dts]
      ,[response_id]
      ,[start_date]
      ,[end_date]
      ,[recorded_date]
      ,[status]
      ,[source]
      ,[IPaddress]
      ,[latitude]
      ,[longitude]
      ,[region]
      ,[duration]
      ,[pageURL]
      ,[external_reference]
      ,[distribution_channel]
      ,[record_source]
      ,[hash_diff]
			   )
	select distinct [surveyresponses].[hash_key]
	,[surveyresponses].[record_load_dts]
      ,[surveyresponses].[responseID]
      ,[surveyresponses].[startdate]
      ,[surveyresponses].[enddate]
      ,[surveyresponses].[recordeddate]
      ,[surveyresponses].[status]
      ,[surveyresponses].[sysEnv]
      ,[surveyresponses].[IPaddress]
      ,[surveyresponses].[locationlatitude]
      ,[surveyresponses].[locationlongitude]
      ,[surveyresponses].[region_name]
      ,[surveyresponses].[duration]
      ,[surveyresponses].[currentPageURL]
      ,[surveyresponses].[pageReferrer]
      ,[surveyresponses].[distributionchannel]
	  ,[surveyresponses].[record_source]
      ,[surveyresponses].[hash_diff]
  FROM [stage].[qualtrics].[surveyresponses]
		left outer join [vault].[raw].[sat_response] ON (
						sat_response.response_hash = surveyresponses.hash_key
					and sat_response.record_load_end_dts IS NULL
					and sat_response.record_source = 'QUALTRICS'
					)
	where	surveyresponses.hash_diff != coalesce(sat_response.hash_diff, '')
	;
	-- select * from [vault].[raw].[sat_response] where record_source = 'QUALTRICS';

	update [vault].[raw].[sat_response]
	set record_load_end_dts = sat_max.record_load_dts
	from [vault].[raw].[sat_response] sat
	inner join
	(
		select response_hash,max(record_load_dts) as record_load_dts
		from [vault].[raw].[sat_response]
		where record_source = 'QUALTRICS'
		group by response_hash
	) as sat_max
	on sat.response_hash = sat_max.response_hash
	and sat.record_load_end_dts is null
	and sat.record_load_dts < sat_max.record_load_dts
	and sat.record_source = 'QUALTRICS'
	;

	----------------------------------------------------------------------

	insert into [vault].[raw].[hub_question_response] (question_response_hash, surveyquestion_id, response_id, record_source, record_load_dts)
	select distinct
		   hash_key
		  ,rtrim(surveyid+Questionid) as surveyQuestionid
          ,ResponseId
		  ,record_source
		  ,record_load_dts
	from [stage].[QUALTRICS].[surveyquestionresponses]
	where not exists (select question_response_hash from [vault].[raw].[hub_question_response] where hub_question_response.question_response_hash = surveyquestionresponses.hash_key and hub_question_response.record_source = 'QUALTRICS')
	;
	-- select * from [vault].[raw].[hub_question_response] where record_source = 'QUALTRICS';

	insert into [vault].[raw].[sat_question_response]
			   ([question_response_hash]
      ,[record_load_dts]
      ,[response_id]
      ,[surveyquestion_id]
      ,[Answer]
      ,[record_source]
      ,[hash_diff]
			   )
	select distinct [surveyquestionresponses].[hash_key]
	,[surveyquestionresponses].[record_load_dts]
      ,[surveyquestionresponses].[responseID]
      ,rtrim([surveyquestionresponses].[surveyid]+[questionid]) surveyquestion_id
      ,[surveyquestionresponses].[value]
	  ,[surveyquestionresponses].[record_source]
      ,[surveyquestionresponses].[hash_diff]
  FROM [stage].[qualtrics].[surveyquestionresponses]
		left outer join [vault].[raw].[sat_question_response] ON (
						sat_question_response.question_response_hash = surveyquestionresponses.hash_key
					and sat_question_response.record_load_end_dts IS NULL
					and sat_question_response.record_source = 'QUALTRICS'
					)
	where	surveyquestionresponses.hash_diff != coalesce(sat_question_response.hash_diff, '')
	;
	-- select * from [vault].[raw].[sat_question_response] where record_source = 'QUALTRICS';

	update [vault].[raw].[sat_question_response]
	set record_load_end_dts = sat_max.record_load_dts
	from [vault].[raw].[sat_question_response] sat
	inner join
	(
		select question_response_hash,max(record_load_dts) as record_load_dts
		from [vault].[raw].[sat_question_response]
		where record_source = 'QUALTRICS'
		group by question_response_hash
	) as sat_max
	on sat.question_response_hash = sat_max.question_response_hash
	and sat.record_load_end_dts is null
	and sat.record_load_dts < sat_max.record_load_dts
	and sat.record_source = 'QUALTRICS'
	;

	----------------------------------------------------------------------

	with surveyquestionresponses as (
	select
        hash_key as questionresponse_hash
        -- Michael Lam: some of the questionid has trailing space created inside the hub_surveyquestion table so space trimming is required to find the correct hash key
		, rtrim(surveyid + questionid) as surveyquestion_id
	    , ResponseId
		, record_source
		, record_load_dts
	from [stage].[QUALTRICS].[surveyquestionresponses]
	),

	members as (
		select responseid
		, hash_key as customer_hash
		from [stage].[QUALTRICS].[members]
	),

	articles as (
		select
			responseid
			, articleid
			, replace(articleID,'.0','') as composer_article_id
		from [stage].[QUALTRICS].[articles]
	),

	composer_articles as (
		select
			content_id
			, COALESCE( hash_key,
				LOWER(
				CONVERT(NVARCHAR(32),
				HASHBYTES('MD5',
				CONVERT(VARBINARY(MAX),
				CONVERT(VARCHAR(MAX), 'COMPOSER' + '|' + CONVERT(NVARCHAR(20), content_id))
				COLLATE SQL_Latin1_General_CP1_CS_AS))
				, 2)
			) ) as article_hash
			, cast(content_id as nvarchar) as composer_article_id
		from [stage].[composer].[composer__articles]
	),

	surveyresponses as (
		select
			responseid
			, hash_key as response_hash
		from [stage].[QUALTRICS].[surveyresponses]
	),

	surveyquestion as (
		select surveyquestionid
			-- Michael Lam: some of the questionid has trailing space created inside the hub_surveyquestion table so space trimming is required to find the correct hash key
			, rtrim(surveyquestionid) as trim_surveyquestion_id
			, hash_key as question_response_hash
		from [stage].[QUALTRICS].[surveyquestion]
	),

	link_exists as (
		select
		link.question_response_hash
		from surveyquestionresponses
		inner join [vault].[raw].[link_surveyquestionresponse] link
		on link.question_response_hash = surveyquestionresponses.questionresponse_hash
		and link.record_source = 'QUALTRICS'
	),

	insert_cte as (
		select
			distinct surveyresponses.response_hash
			, surveyquestionresponses.questionresponse_hash
			, members.customer_hash
			, surveyquestion.question_response_hash
			, composer_articles.article_hash
			, surveyquestionresponses.surveyquestion_id
			, surveyquestionresponses.ResponseId
			, surveyquestionresponses.record_source
			, surveyquestionresponses.record_load_dts
		from surveyquestionresponses
		inner join members on members.responseid = surveyquestionresponses.ResponseId
		inner join articles on articles.responseid = surveyquestionresponses.Responseid
		inner join composer_articles on composer_articles.composer_article_id = articles.composer_article_id
		inner join surveyresponses on surveyresponses.responseid = surveyquestionresponses.responseid
		inner join surveyquestion on surveyquestion.trim_surveyquestion_id = surveyquestionresponses.surveyquestion_id
		-- check if link already exist in link table, if so exclude it from insert:
		left join link_exists on link_exists.question_response_hash = surveyquestionresponses.questionresponse_hash
		where link_exists.question_response_hash is null

-- switch data over,
-- add schedule to run after qualtrics alteryx
-- update load vault for qualtrics & composer
-- update load vault to run after qualtrics & composer

		-- testing
		-- inner join members on members.responseid = surveyquestionresponses.ResponseId -- inner: 23,814 | left: 23,814
    	-- inner join articles on articles.responseid = surveyquestionresponses.Responseid -- inner: 23,814 | left: 23,814
    	-- left join composer_articles on composer_articles.composer_article_id = articles.composer_article_id -- inner: 13,692 | left: 23,814 :( problem here
    	-- inner join surveyresponses on surveyresponses.responseid = surveyquestionresponses.responseid -- inner: 23,814 | left: 23,814
    	-- inner join surveyquestion on surveyquestion.trim_surveyquestion_id = surveyquestionresponses.surveyquestion_id -- inner: 23,814 | left: 23,814
    	-- check if link already exist in link table:
    	-- left join link_exists on link_exists.question_response_hash = surveyquestionresponses.questionresponse_hash -- inner: 13,692 | left: 23,814 (inner is matched to 13k correctly)
    	-- where link_exists.question_response_hash is null -- 10,122 rows
	)

	insert into [vault].[raw].[link_surveyquestionresponse] (
		response_hash
		,question_response_hash
		,customer_hash
		,question_hash
		,article_hash
		,surveyquestion_id
		,response_id
		,record_source
		,record_load_dts
	)

    select * from insert_cte
	;
	-- 56 surveyquestionresponsess with no customers out of 50,000 (0,1%)

	--  select count(1),count(distinct subscription_hash) from [vault].[raw].[link_subscription] where record_source = 'QUALTRICS';
	--  select top 1000 * from [vault].[raw].[link_subscription] where record_source = 'QUALTRICS' order by 2;


	----------------------------------------------------------------------
end

	insert into [stage].[log].[etl_job] ([user],[source],[job],[status]) values (CURRENT_USER,OBJECT_SCHEMA_NAME(@@PROCID),OBJECT_NAME(@@PROCID),'finished');
GO
