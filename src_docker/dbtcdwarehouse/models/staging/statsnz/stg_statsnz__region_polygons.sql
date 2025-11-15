WITH region_poly AS (
    SELECT
        REGC2023_2 AS region
        , ST_GeogFromGeoJson(geom) AS region_polygon
    FROM {{ source('statsnz', 'nz_regional_council_2023_generalised') }}
)

SELECT * FROM region_poly
