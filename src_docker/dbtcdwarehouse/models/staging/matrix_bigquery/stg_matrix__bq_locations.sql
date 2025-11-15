{{
  config(
    tags=['bq_matrix']
  )
}}
WITH locations AS (
    SELECT * FROM {{ source('bq_matrix', 'tbl_location') }}
)

, object_links AS (
    SELECT * FROM {{ source('bq_matrix', 'tbl_ObjectLink') }}
)

, linktype AS (
    SELECT * FROM {{ source('bq_matrix', 'TBL_LINKType') }}
)

, persons AS (
    SELECT * FROM {{ source('bq_matrix', 'tbl_person') }}
)

SELECT
    object_links.Level1Pointer
    , locations.loc_pointer
    , locations.Country_Code
    , locations.AddrLine1
    , locations.AddrLine2
    , locations.AddrLine3
    , locations.AddrLine4
    , locations.AddrLine5
    , locations.AddrLine6
    , locations.ObjectPointer
    , locations.Latitude
    , locations.Longitude
    , ROW_NUMBER() OVER (PARTITION BY persons.ObjectPointer ORDER BY locations.ObjectPointer DESC) AS rn
FROM
    persons
INNER JOIN object_links
    ON persons.ObjectPointer = object_links.Level1Pointer
INNER JOIN locations
    ON object_links.LinkObject = locations.ObjectPointer
INNER JOIN linktype
    ON object_links.LinkType = linktype.LinkType
WHERE
    linktype.ReqPer = 1
    AND object_links.ToDate IS NULL
QUALIFY rn = 1
