{{ config(
  tags = ['piano_braze_sync_mart'],
  materialized = 'incremental',
  partition_by = {
    'field': 'updated_at',
    'data_type': 'timestamp',
    'granularity': 'day'
  },
  unique_key = 'external_id',
  on_schema_change = 'sync_all_columns'
) }}

{% set prev_exists_query %}
  SELECT COUNT(*) > 0 AS table_exists
  FROM `{{ this.schema }}.INFORMATION_SCHEMA.TABLES`
  WHERE table_name = '{{ this.table }}'
{% endset %}

{% set prev_exists_result = run_query(prev_exists_query) %}
{% if prev_exists_result %}
  {% set prev_exists = prev_exists_result.columns[0].values[0] %}
{% else %}
  {% set prev_exists = false %}
{% endif %}

WITH subscription_data AS (

  SELECT
    {{ format_uuid(generate_uuid_v5("user_id__uid_")) }} AS external_id,
    ARRAY_AGG(
      STRUCT(
        {{ format_timestamp("create_date") }} AS create_date,
        subscription_id AS subscription_id,
        {{ format_timestamp("start_date") }} AS start_date,
        {{ format_timestamp("end_date") }} AS end_date,
        days_subscribed AS days_subscribed,
        subscription_status AS subscription_status,
        upgrade_status AS upgrade_status,
        trial_status AS trial_status,
        {{ format_timestamp("trial_period_end_date") }} AS trial_period_end_date,
        trial_price AS trial_price,
        regular_price AS regular_price,
        auto_renew AS auto_renew,
        {{ format_timestamp("auto_renew_disablement_date") }} AS auto_renew_disablement_date,
        {{ format_timestamp("last_active_date") }} AS last_active_date,
        sessions_last_30_days AS sessions_last_30_days,
        pageviews_last_30_days AS pageviews_last_30_days,
        billing_period AS billing_period,
        total_charged AS total_charged,
        charge_count AS charge_count,
        total_refunded AS total_refunded,
        {{ format_timestamp("last_billing_date") }} AS last_billing_date,
        {{ format_timestamp("next_billing_date") }} AS next_billing_date,
        renewed AS renewed,
        currently_in_grace_period AS currently_in_grace_period,
        {{ format_timestamp("grace_period_start_date") }} AS grace_period_start_date,
        {{ format_timestamp("grace_period_extended_to") }} AS grace_period_extended_to,
        user_id__uid_ AS user_id__uid_,
        {{ format_timestamp("access_expiration_date") }} AS access_expiration_date,
        resource_id__rid_ AS resource_id__rid_,
        resource_name AS resource_name,
        term_id AS term_id,
        term_name AS term_name,
        term_type AS term_type,
        template_id AS template_id,
        template_name AS template_name,
        offer_id AS offer_id,
        offer_name AS offer_name,
        experience_id AS experience_id,
        experience_name AS experience_name,
        campaign_codes AS campaign_codes,
        promo_code AS promo_code,
        conversion_city AS conversion_city,
        conversion_state AS conversion_state,
        conversion_country AS conversion_country,
        cleaned_url AS cleaned_url,
        company_name AS company_name,
        CASE
          WHEN LOWER(postal_code) = 'nan' THEN NULL
          ELSE SAFE_CAST(SAFE_CAST(SAFE_CAST(postal_code AS FLOAT64) AS INT64) AS STRING)
        END AS postal_code,
        CASE
          WHEN phone IS NULL THEN NULL
          ELSE SAFE_CAST(SAFE_CAST(phone AS INT64) AS STRING)
        END AS phone,
        CASE
          WHEN LOWER(billing_postal_code) = 'nan' THEN NULL
          ELSE SAFE_CAST(SAFE_CAST(SAFE_CAST(billing_postal_code AS FLOAT64) AS INT64) AS STRING)
        END AS billing_postal_code,
        shared_subscriptions AS shared_subscriptions,
        created_via_upgrade AS created_via_upgrade,
        period_name AS period_name,
        access_period AS access_period,
        period_count AS period_count,
        app_name AS app_name
      )
    ) AS subscriptions
  FROM
    (
      SELECT
        pvl.*,
        ROW_NUMBER() over (
          PARTITION BY pvl.user_id__uid_, pvl.app_name
          ORDER BY pvl.create_date DESC,
          pvl.subscription_id DESC
        ) AS rn
      FROM
        {{ source(
          'piano',
          'piano__vxsubscriptionlog'
        ) }} pvl
      WHERE
        pvl.user_id__uid_ IS NOT NULL
    )
  WHERE
    rn = 1
  GROUP BY
    user_id__uid_
),

current_data AS (
  SELECT
    external_id,
    json_object(
      'digital_product_subscription',
      subscriptions
    ) AS payload,
    TO_BASE64(MD5(TO_JSON_STRING(json_object('digital_product_subscriptions', subscriptions)))) AS subscription_hash,
    parse_timestamp('%Y-%m-%d %H:%M:%S %Z', format_timestamp('%Y-%m-%d %H:%M:%S %z', current_timestamp())) AS current_run_timestamp
  FROM
    subscription_data
  WHERE
    ARRAY_LENGTH(subscriptions) >= 1
),

previous_data AS (
  {% if is_incremental() %}
  SELECT
    external_id,
    subscription_hash,
    updated_at
  FROM {{ this }}
  {% else %}
  SELECT
    CAST(NULL AS STRING) AS external_id,
    CAST(NULL AS STRING) AS subscription_hash,
    CAST(NULL AS TIMESTAMP) AS updated_at
 LIMIT 0
  {% endif %}
)

SELECT
  curr.external_id,
  curr.subscription_hash,
  curr.payload,
  CASE
    WHEN prev.external_id IS NULL THEN curr.current_run_timestamp
    WHEN curr.subscription_hash = prev.subscription_hash THEN prev.updated_at
    ELSE curr.current_run_timestamp
  END AS updated_at
FROM current_data curr
LEFT JOIN previous_data prev ON curr.external_id = prev.external_id

{% if is_incremental() %}
WHERE prev.external_id IS NULL or curr.subscription_hash != prev.subscription_hash
{% endif %}
