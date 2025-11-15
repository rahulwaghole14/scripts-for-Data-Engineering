{{
    config(
        tags=['hexaevent'],
    )
}}

with source as (
      select DATE_TRUNC(DATE("2014-01-01"), YEAR) AS event_year
      , bib_ AS bib_number
        , lower(trim(gender)) AS gender
      , PARSE_DATE('%d-%b-%y', dob) AS date_of_birth
      from {{ source('hexaevent', 'hexaevent_rtb__RTB14___Entries') }}
      union all
      select DATE_TRUNC(DATE("2015-01-01"), YEAR) AS event_year
      , bib_ AS bib_number
        , lower(trim(gender)) AS gender
      , PARSE_DATE('%d/%m/%Y', dob) AS date_of_birth
      from {{ source('hexaevent', 'hexaevent_rtb__RTB15___Entries') }}
        union all
        select DATE_TRUNC(DATE("2016-01-01"), YEAR) AS event_year
        , bib_number AS bib_number
        , lower(trim(gender)) AS gender
        , PARSE_DATE('%d/%m/%Y', date_of_birth) AS date_of_birth
        from {{ source('hexaevent', 'hexaevent_rtb__RTB16___Entries') }}

        union all
        select DATE_TRUNC(DATE("2017-01-01"), YEAR) AS event_year
        , bib AS bib_number
        , lower(gender) AS gender
        , PARSE_DATE('%d/%m/%Y', date_of_birth) AS date_of_birth
        from {{ source('hexaevent', 'hexaevent_rtb__RTB17___Entries') }}
        union all
        select DATE_TRUNC(DATE("2018-01-01"), YEAR) AS event_year
        , bib AS bib_number
        , lower(gender) AS gender
        , PARSE_DATE('%d/%m/%Y', date_of_birth) AS date_of_birth
        from {{ source('hexaevent', 'hexaevent_rtb__RTB18___Entries' ) }}

        union all
        select DATE_TRUNC(DATE("2019-01-01"), YEAR) AS event_year
        , bib AS bib_number
        , lower(gender) AS gender
        , PARSE_DATE('%d/%m/%Y', date_of_birth) AS date_of_birth
        from {{ source('hexaevent', 'hexaevent_rtb__RTB19___Entries' ) }}


        union all
        select DATE_TRUNC(DATE("2020-01-01"), YEAR) AS event_year
        , bib AS bib_number
        , lower(gender) AS gender
        , PARSE_DATE('%d/%m/%Y', date_of_birth) AS date_of_birth
        from {{ source('hexaevent', 'hexaevent_rtb__RTB2020___Entries' ) }}

        union all
        select DATE_TRUNC(DATE("2021-01-01"), YEAR) AS event_year
        , bib AS bib_number
        , lower(gender) AS gender
        , PARSE_DATE('%d/%m/%Y', date_of_birth) AS date_of_birth
        from {{ source('hexaevent', 'hexaevent_rtb__RTB21___Entries__Virtual_' ) }}

        union all
        select DATE_TRUNC(DATE("2021-01-01"), YEAR) AS event_year
        , bib AS bib_number
        , lower(gender) AS gender
        , PARSE_DATE('%d/%m/%Y', date_of_birth) AS date_of_birth
        from {{ source('hexaevent', 'hexaevent_rtb__RTB21___Entries_In_Person__Pre_Refunds_' ) }}

        union all
        select DATE_TRUNC(DATE("2022-01-01"), YEAR) AS event_year
        , bib AS bib_number
        , lower(gender) AS gender
        , PARSE_DATE('%d/%m/%Y', date_of_birth) AS date_of_birth
        from {{ source('hexaevent', 'hexaevent_rtb__RTB22___All_Entries__Pre_Refunds_' ) }}

        union all
        select DATE_TRUNC(DATE("2022-01-01"), YEAR) AS event_year
        , bib AS bib_number
        , lower(gender) AS gender
        , PARSE_DATE('%d/%m/%Y', date_of_birth) AS date_of_birth
        from {{ source('hexaevent', 'hexaevent_rtb__RTB22___Entries__Virtual_' ) }}

        union all
        select DATE_TRUNC(DATE("2023-01-01"), YEAR) AS event_year
        , bib AS bib_number
        , lower(gender) AS gender
        , PARSE_DATE('%d/%m/%Y', date_of_birth) AS date_of_birth
        from {{ source('hexaevent', 'hexaevent_rtb__RTB23___Entries' ) }}

        union all
        select DATE_TRUNC(DATE("2024-01-01"), YEAR) AS event_year
        , bib AS bib_number
        , lower(gender) AS gender
        , PARSE_DATE('%d/%m/%Y', date_of_birth) AS date_of_birth
        from {{ source('hexaevent', 'hexaevent_rtb__RTB24___Entries_' ) }}

)

select *
  , DATE_DIFF(event_year, date_of_birth, YEAR) AS age
from source
