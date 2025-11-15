{{ config(
  tags = ['bq_matrix'],
  materialized = 'incremental',
  partition_by ={ 'field': 'updated_at',
  'data_type': 'timestamp',
  'granularity': 'day' },
  unique_key = 'external_id'
) }}

WITH matrix_subscription_details AS (

  SELECT
    DISTINCT subs_pointer,
    paytype_id,
    productid,
    exp_end_date,
    serviceid,
    sord_startdate,
    period_id,
    camp_id,
    canx_date,
    canx_id,
    update_time
  FROM
    {{ ref('int_matrix__subscription_details') }}
),

marketing_id AS (
  SELECT
    DISTINCT LOWER(marketing_id) AS marketing_id,
    subs_id
  FROM
    {{ ref('int_matrix__to_braze_user_profile') }}
  WHERE
    marketing_id IS NOT NULL
),

rate_rounds AS (
  SELECT
    DISTINCT subs_id,
    product_id,
    rate_head_id AS print__rate_id,
    round_id AS print__round_id
  FROM
    {{ ref('stg_matrix__bq_rpt_subscribers_rate_rounds') }}
),

subs_id AS (
  SELECT
    DISTINCT subs_id,
    subs_pointer
  FROM
    {{ ref('stg_matrix__bq_subscribers') }}
),

joined_data AS (
  SELECT
    DISTINCT marketing_id.marketing_id,
    matrix_subscription_details.subs_pointer,
    matrix_subscription_details.serviceid AS print__service_id,
    matrix_subscription_details.productid AS print__product_id,
    matrix_subscription_details.paytype_id AS print__payment_type,
    matrix_subscription_details.camp_id AS print__campaign_code,
    matrix_subscription_details.period_id AS print__sub_period,
    matrix_subscription_details.canx_id AS print__cancel_id,
    CAST(
      matrix_subscription_details.exp_end_date AS STRING
    ) AS print__exp_end_date,
    CAST(
      matrix_subscription_details.sord_startdate AS STRING
    ) AS print__start_date,
    CAST(
      matrix_subscription_details.canx_date AS STRING
    ) AS print__cancel_date,
    rate_rounds.print__rate_id,
    rate_rounds.print__round_id,
    matrix_subscription_details.update_time AS update_time
  FROM
    marketing_id
    LEFT JOIN subs_id
    ON subs_id.subs_id = marketing_id.subs_id
    LEFT JOIN matrix_subscription_details
    ON matrix_subscription_details.subs_pointer = subs_id.subs_pointer
    LEFT JOIN rate_rounds
    ON rate_rounds.subs_id = subs_id.subs_id
    AND rate_rounds.product_id = matrix_subscription_details.productid
    WHERE
      matrix_subscription_details.subs_pointer IS NOT NULL
    AND
      matrix_subscription_details.productid IS NOT NULL
),

subscription_data AS (
  SELECT
    LOWER(marketing_id) AS external_id,
    MAX(update_time) AS updated_at,
    ARRAY_AGG(
      STRUCT(
        print__payment_type AS print__payment_type,
        print__product_id AS print__product_id,
        print__rate_id AS print__rate_id,
        {{ format_timestamp("print__exp_end_date") }} AS print__exp_end_date,
        print__round_id AS print__round_id,
        print__service_id AS print__service_id,
        {{ format_timestamp("print__start_date") }} AS print__start_date,
        print__sub_period AS print__sub_period,
        print__campaign_code AS print__campaign_code,
        {{ format_timestamp("print__cancel_date") }} AS print__cancel_date,
        print__cancel_id AS print__cancel_id
      )
    ) AS subscriptions
  FROM
    joined_data
  GROUP BY
    marketing_id
)

SELECT
  external_id,
  updated_at,
  json_object(
    'print_product_subscription',
    subscriptions
  ) AS payload
FROM
  subscription_data
WHERE
  array_length(subscriptions) >= 1
