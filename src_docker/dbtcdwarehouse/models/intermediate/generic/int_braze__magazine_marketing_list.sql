{{
  config(
        tags=['braze']
    )
}}

with blacklist as (
    select LOWER(marketing_id) as marketing_id,
           MAX(updated_at) AS updated_at
    from {{ ref('stg_acm__print_data_blacklist') }}
    where CAST(status AS INT64) in (3,4)
    GROUP BY LOWER(marketing_id)
)

, mag_subs as (

    SELECT
        print_data_generic.marketing_id as marketing_id
        , case
            when print_data_generic.internal_name = 'nzHouseGardenNewsletter' then 'nz-house-and-garden_marketing_newsletter_nz-house-and-garden'
            when print_data_generic.internal_name = 'magsGiftsMarketing' then 'mags4gifts_marketing_newsletter_mags4gifts-newsletter'
            when print_data_generic.internal_name = 'tvGuideNewsletter' then 'tv-guide_marketing_newsletter_tv-guide-newsletter'
            when print_data_generic.internal_name = 'HouseofWellnessNewsletter' then 'house-of-wellness_marketing_newsletter_houseofwellness-newsletter'
            when print_data_generic.internal_name = 'ChemistWarehouseSubs' then 'house-of-wellness_marketing_newsletter_houseofwellness-newsletter'
            when print_data_generic.internal_name = 'getGrowingNewsletter' then 'nz-gardener_marketing_newsletter_get-growing'
            else 'unknown' end as internal_name,
        GREATEST(
            COALESCE(print_data_generic.updated_at, TIMESTAMP '1970-01-01'),
            COALESCE(bl.updated_at, TIMESTAMP '1970-01-01')
        ) as updated_at
    FROM {{ ref('stg_acm__print_data_generic') }} print_data_generic
    LEFT JOIN blacklist bl ON print_data_generic.marketing_id = bl.marketing_id
    WHERE  bl.marketing_id IS NULL
    and internal_name in (
    'nzHouseGardenNewsletter',
    'magsGiftsMarketing',
    'tvGuideNewsletter',
    'HouseofWellnessNewsletter',
    'ChemistWarehouseSubs',
    'getGrowingNewsletter'
    )
)

select
    distinct lower(marketing_id) as marketing_id
    , internal_name
    , updated_at
from mag_subs
