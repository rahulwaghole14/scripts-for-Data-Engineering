{{
  config(
    tags='bq_matrix'
  )
}}

WITH subscriptions AS (
    SELECT * FROM {{ ref('stg_matrix__bq_subscriptions') }}
)

, products AS (
    SELECT * FROM {{ ref('stg_matrix__bq_products') }}
)

, services AS (
    SELECT * FROM {{ ref('stg_matrix__bq_services') }}
)

, subscribers AS (
    SELECT * FROM {{ ref('stg_matrix__bq_subscribers') }}
)

, persons AS (
    SELECT * FROM {{ ref('stg_matrix__bq_persons') }}
)


SELECT
DISTINCT
    NULL AS SUBSCRIPTION_STATUS
    , subscriptions.sord_startdate AS SUBSCRIPTION_START_DATE
    , subscriptions.sord_stopdate AS SUBSCRIPTION_END_DATE
    , subscriptions.sord_id AS SUBSCRIPTION_ID
    , products.ProductID AS PRODUCT_ID
    , products.ProductDesc AS PRODUCT_DESCRIPTION
    , services.ServiceID AS SERVICE_ID
    , services.ServiceDesc AS SERVICE_DESCRIPTION
    , services.Mon
    , services.Tue
    , services.Wed
    , services.Thu
    , services.Fri
    , services.Sat
    , services.Sun
    , subscribers.subtype_id AS CUSTOMER_TYPE
    , persons.PersContact3 AS EMAIL
    , subscribers.subs_id AS CUSTOMER_ID
FROM subscriptions
INNER JOIN services
    ON subscriptions.serviceID = services.serviceID
INNER JOIN products
    ON subscriptions.productID = products.productID
INNER JOIN subscribers
    ON subscriptions.subs_pointer = subscribers.subs_pointer
LEFT JOIN persons
    ON subscribers.subs_perid = persons.person_pointer
WHERE subscriptions.sord_stopdate IS null OR subscriptions.sord_stopdate >= CURRENT_DATE('Pacific/Auckland')
