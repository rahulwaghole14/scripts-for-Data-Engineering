{{
    config(
        tags = ['drupal_content']
    )
}}

with source as (

      select * from {{ source('drupal_dpa', 'drupal__articles') }}

)

, authors AS (

    SELECT
      source.ID AS ARTICLE_ID,
      STRING_AGG(AUTHOR_STRUCT.NAME, ', ' ORDER BY AUTHOR_STRUCT.NAME) AS AUTHOR_NAMES,
      STRING_AGG(CAST(AUTHOR_STRUCT.ID AS STRING), ', ' ORDER BY AUTHOR_STRUCT.NAME) AS AUTHOR_IDS
    FROM
      source,
      UNNEST(source.AUTHOR) AS AUTHOR_STRUCT
    GROUP BY
      source.ID

)

, renamed as (

    select
        SAFE_CAST(source.{{ adapter.quote("id") }} AS INT64) AS ARTICLE_KEY,
        ANY_VALUE(source.{{ adapter.quote("id") }}) AS ARTICLE_ID,
        ANY_VALUE(source.{{ adapter.quote("drupalId") }}) AS ARTICLE_DRUPALID,
        ANY_VALUE(source.{{ adapter.quote("contentType") }}) AS ARTICLE_CONTENTTYPE,
        ANY_VALUE(source.{{ adapter.quote("url") }}) AS ARTICLE_URL,
        ANY_VALUE(source.{{ adapter.quote("headline") }}) AS ARTICLE_HEADLINE,
        ANY_VALUE(source.{{ adapter.quote("headlinePrimary") }}) AS ARTICLE_HEADLINEPRIMARY,
        ANY_VALUE(source.{{ adapter.quote("slug") }}) AS ARTICLE_SLUG,
        ANY_VALUE(source.{{ adapter.quote("byline") }}.name) AS ARTICLE_BYLINE,
        ANY_VALUE(source.{{ adapter.quote("status") }}) AS ARTICLE_STATUS,
        ANY_VALUE(source.{{ adapter.quote("createdDate") }}) AS ARTICLE_CREATEDDATE,
        ANY_VALUE(source.{{ adapter.quote("publishedDate") }}) AS ARTICLE_PUBLISHEDDATE,
        ANY_VALUE(source.{{ adapter.quote("updatedDate") }}) AS ARTICLE_UPDATEDDATE,
        ANY_VALUE(source.{{ adapter.quote("teaser") }}.shortHeadline) AS ARTICLE_TEASER_SHORTHEADLINE,
        ANY_VALUE(source.{{ adapter.quote("teaser") }}.intro) AS ARTICLE_TEASER_INTRO,
        ANY_VALUE(source.{{ adapter.quote("body") }}) AS ARTICLE_BODY,
        STRING_AGG(body_struct.{{ adapter.quote("text") }},
         ', ' ORDER BY body_struct.{{ adapter.quote("text") }}) AS ARTICLE_BODY_AGG,
        ANY_VALUE(source.{{ adapter.quote("source") }}) AS ARTICLE_SOURCE,
        ANY_VALUE(source.{{ adapter.quote("section") }}) AS ARTICLE_SECTION,
        ANY_VALUE(source.{{ adapter.quote("teams") }}) AS ARTICLE_TEAMS,
        STRING_AGG(DISTINCT teams_struct, ', ' ORDER BY teams_struct) AS ARTICLE_TEAMS_AGG,
        ANY_VALUE(source.{{ adapter.quote("typeOfWorkLabel") }}) AS ARTICLE_TYPEOFWORKLABEL,
        STRING_AGG(DISTINCT typeofworklabel_struct, ', ' ORDER BY typeofworklabel_struct) AS ARTICLE_TYPEOFWORKLABEL_AGG,
        ANY_VALUE(source.{{ adapter.quote("liveBlog") }}) AS ARTICLE_LIVEBLOG,
        ANY_VALUE(source.{{ adapter.quote("searchIndexed") }}) AS ARTICLE_SEARCHINDEXED,
        ANY_VALUE(source.{{ adapter.quote("topics") }}) AS ARTICLE_TOPICS,
        STRING_AGG(DISTINCT topics_struct, ', ' ORDER BY topics_struct) AS ARTICLE_TOPICS_AGG,
        ANY_VALUE(source.{{ adapter.quote("entities") }}) AS ARTICLE_ENTITIES,
        STRING_AGG(DISTINCT entities_struct, ', ' ORDER BY entities_struct) AS ARTICLE_ENTITIES_AGG,
        ANY_VALUE(source.{{ adapter.quote("sensitivity") }}) AS ARTICLE_SENSITIVITY,
        ANY_VALUE(source.{{ adapter.quote("sentiment") }}) AS ARTICLE_SENTIMENT,
        ANY_VALUE(source.{{ adapter.quote("userNeed") }}) AS ARTICLE_USERNEED,
        ANY_VALUE(source.{{ adapter.quote("newsValue") }}) AS ARTICLE_NEWSVALUE,
        ANY_VALUE(source.{{ adapter.quote("lifetime") }}) AS ARTICLE_LIFETIME,
        ANY_VALUE(source.{{ adapter.quote("comments") }}) AS ARTICLE_COMMENTS,
        ANY_VALUE(source.{{ adapter.quote("mainPublicationChannel") }}.name) AS ARTICLE_MAINPUBLICATIONCHANNEL_NAME,
        ANY_VALUE(source.{{ adapter.quote("mainPublicationChannel") }}.key) AS ARTICLE_MAINPUBLICATIONCHANNEL_KEY,
        ANY_VALUE(authors.AUTHOR_IDS) AS ARTICLE_AUTHOR_ID,
        ANY_VALUE(authors.AUTHOR_NAMES) AS ARTICLE_AUTHOR_NAME,
        CASE WHEN ANY_VALUE(source.{{ adapter.quote("updatedDate") }}) >
        ANY_VALUE(source.{{ adapter.quote("publishedDate") }})
        THEN ANY_VALUE(TIMESTAMP(source.{{ adapter.quote("updatedDate") }}))
        ELSE ANY_VALUE(TIMESTAMP(source.{{ adapter.quote("publishedDate") }})) END AS EFFECTIVE_FROM

    from source
    LEFT JOIN UNNEST({{ adapter.quote("body") }}) AS body_struct WITH OFFSET AS body_offset ON TRUE
    LEFT JOIN UNNEST({{ adapter.quote("entities") }}) AS entities_struct WITH OFFSET AS entities_offset ON TRUE
    LEFT JOIN UNNEST({{ adapter.quote("teams") }}) AS teams_struct WITH OFFSET AS teams_offset ON TRUE
    LEFT JOIN UNNEST({{ adapter.quote("topics") }}) AS topics_struct WITH OFFSET AS topics_offset ON TRUE
    LEFT JOIN UNNEST({{ adapter.quote("typeOfWorkLabel") }}) AS typeofworklabel_struct WITH OFFSET AS typeofworklabel_offset ON TRUE
    LEFT JOIN authors on authors.ARTICLE_ID = source.ID

    GROUP BY 1

)

select * from renamed
