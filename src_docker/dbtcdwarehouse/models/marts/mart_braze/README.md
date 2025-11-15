# Braze Cloud Data Ingestion (CDI) and dbt Snapshots

## use cases:
- batch sync: sync all records that have changed since the last sync daily for a specific set of attributes or nested attributes keys, only sync attributes or nested attributes keys that have changed
- retrospective backfill sync for new fields: sync all records for a specific date range and for a specific subset of attributes or nested attributes keys

## Proposed solution design for Braze data sync use cases
- Use [dbt Snapshots](https://docs.getdbt.com/docs/build/snapshots) or data vault: to track changes in your user profiles table, including nested attributes. (bigquery) Update the snapshots after the data sync from the source system to the data warehouse.
- Flatten or Serialize Nested Fields: using dbt convert nested fields into JSON strings to facilitate change detection. or use a flat structure to detect against each field (bigquery)
- Detect Changes: Configure the vault/snapshot to identify records where any attribute has changed since the last sync. (bigquery)
- Create a Table for Changed Records: Build a table/view or query that filters out only the records that have changed and effectively only select nested attribute keys that have changed, ignoring unchanged keys (braze consumes a data point for each key send in the payload)  (bigquery)
- Sync Changed Records with Braze: Use the table/view as the source for syncing data to Braze, following their data ingestion requirements. (braze)

## Overview of Braze cloud_ingestion
- [LINK](https://www.braze.com/docs/user_guide/data_and_analytics/cloud_ingestion/overview)
With Braze Cloud Data Ingestion (CDI), you set up an integration between your data warehouse instance and Braze workspace to sync data on a recurring basis. This sync runs on a schedule you set, and each integration can have a different schedule. Syncs can run as frequently as every 15 minutes or as infrequently as once per month. For customers who need syncs to occur more frequently than 15 minutes, please speak with your customer success manager, or consider using REST API calls for real-time data ingestion.

## Update and merge via API:
To update an existing object, send a POST to `users/track` with the `_merge_objects` parameter in the request. This will deep merge your update with the existing object data. Deep merging ensures that all levels of an object are merged into another object instead of only the first level.

## Support for methods in the Cloud data warehouse integration
In the Cloud data warehouse integration it also supports api methods i.e.:
also see [nested_custom_attribute_support](https://braze.com/docs/user_guide/data_and_analytics/custom_data/custom_attributes/nested_custom_attribute_support/) and [array_of_objects](https://www.braze.com/docs/user_guide/data_and_analytics/custom_data/custom_attributes/array_of_objects/) for more information about this.

- `_merge_objects` You must set `_merge_objects` to `true`, or your objects will be overwritten aside from initial creation. `_merge_objects` is `false` by default.
- `_update_existing_only` If you set `_update_existing_only` to `true`, the API will only update existing objects and will not create new objects. If you set `_update_existing_only` to `false`, the API will update existing objects and create new objects. `_update_existing_only` is `true` by default. (not sure if supported via CDI)
- `$add` Add another item to the array using the `$add` operator.
- `$update` Update values for specific objects within an array using the _merge_objects parameter and the $update operator. Similar to updates to simple nested custom attribute objects, this performs a deep merge.
- `$identifier_key`, `$identifier_value`
- `$new_object`
- `$remove` Remove objects from an array using the operator in combination with a matching key (`$identifier_key`) and value (`$identifier_value`). (not sure if supported via CDI)
- `$time` To capture dates as object properties, you must use the `$time` key.

You must set `_merge_objects` to `true`, or your objects will be overwritten. `_merge_objects` is `false` by default.

### Capturing dates as object properties
To capture dates as object properties, you must use the `$time` key. In the following example, an “Important Dates” object is used to capture the set of object properties, birthday and wedding_anniversary. The value for these dates is an object with a `$time` key, which cannot be a null value.

```json
{
  "attributes": [
    {
      "external_id": "time_with_nca_test",
      "important_dates": {
        "birthday": {"$time" : "1980-01-01"},
        "wedding_anniversary": {"$time" : "2020-05-28"}
      }
    }
  ]
}
```

see [this](https://www.braze.com/docs/api/objects_filters/user_attributes_object) for more detail on data expectations in braze and also a list of built-in attributes for braze user objects i.e. `first_name`, `email`.

### examples for CDI sync:
we want to add an array of pets owned by each user, which corresponds to owner_id. Specifically, we want to include identification, breed, type, and name. We can use the following payload and sync to braze:

```json
UPDATED_AT	EXTERNAL_ID	PAYLOAD
2023-10-02 19:56:17.377 +0000	03409324	{"_merge_objects":"true","pets":{"$add":[{"breed":"parakeet","id":5,"name":"Mary","type":"bird"}]}}
2023-10-02 19:56:17.377 +0000	21231234	{"_merge_objects":"true","pets":{"$add":[{"breed":"calico","id":2,"name":"Gerald","type":"cat"},{"breed":"beagle","id":1,"name":"Gus","type":"dog"}]}}
2023-10-02 19:56:17.377 +0000	12335345	{"_merge_objects":"true","pets":{"$add":[{"breed":"corgi","id":3,"name":"Doug","type":"dog"},{"breed":"salmon","id":4,"name":"Larry","type":"fish"}]}}
```

Next, to send an updated name field and new age field for each owner, we can use the following query to populate a table or view:

```json
UPDATED_AT	EXTERNAL_ID	PAYLOAD
2023-10-02 19:50:25.266 +0000	03409324	{"_merge_objects":"true","pets":{"$update":[{"$identifier_key":"id","$identifier_value":5,"$new_object":{"age":7,"name":"Mary"}}]}}
2023-10-02 19:50:25.266 +0000	21231234	{"_merge_objects":"true","pets":{"$update":[{"$identifier_key":"id","$identifier_value":2,"$new_object":{"age":3,"name":"Gerald"}},{"$identifier_key":"id","$identifier_value":1,"$new_object":{"age":3,"name":"Gus"}}]}}
2023-10-02 19:50:25.266 +0000	12335345	{"_merge_objects":"true","pets":{"$update":[{"$identifier_key":"id","$identifier_value":3,"$new_object":{"age":6,"name":"Doug"}},{"$identifier_key":"id","$identifier_value":4,"$new_object":{"age":1,"name":"Larry"}}]}}
```

## About nested custom attributes
- [nested_custom_attribute_support](https://braze.com/docs/user_guide/data_and_analytics/custom_data/custom_attributes/nested_custom_attribute_support/)
Note: Any attribute/nested key that is updated consumes a data point. For example, if you update a nested key, the entire key path is consumed. For example, if you update `key1 + key2 + key3`, all three keys are consumed. If you update `key1 + key2`, only `key1` and `key2` are consumed as data points.

Data point billing for Cloud Data Ingestion is equivalent to billing for updates through the /users/track endpoint. Refer to [Data points](https://www.braze.com/docs/user_guide/data_and_analytics/data_points) for more information.


## Cloud Data Ingestion Endpoints
Use the Braze Cloud Data Ingestion endpoints to manage your data warehouse integrations and syncs. For example these could be used to programmatically trigger a sync, check the status of a sync, or list all integrations.

- [overview](https://www.braze.com/docs/api/endpoints/cdi)
- [get_integration_list](https://www.braze.com/docs/api/endpoints/cdi/get_integration_list)
- [get_job_sync_status](https://www.braze.com/docs/api/endpoints/cdi/get_job_sync_status)
- [post_job_sync](https://www.braze.com/docs/api/endpoints/cdi/post_job_sync)

## old setup
for the old setup we used the following dbt models  to sync the data to braze using hightouch - to sync to braze object(s).

- account management user profiles `src_docker/dbtcdwarehouse/models/intermediate/generic/int_account_management__user_profiles.sql` (hightouch ref: cdw_to_braze__idm_user_profiles -> prod__hexa_prod__braze)
- account management subscription group membership status `src_docker/dbtcdwarehouse/models/intermediate/generic/int_account_management__sub_status.sql` (hightouch ref: cdw_to_braze__idm_sub_status -> prod__hexa_prod__braze, braze sub group id acaa104a-65ad-47e1-84e7-3ed6359cce60)
- matrix
- presspatron

## pushing in nested custom attributes as objects:
```sql
    , STRUCT(
        ARRAY(
            SELECT AS STRUCT
                JSON_VALUE(addresses_json, '$.postalCode') AS postal_code
                , JSON_VALUE(addresses_json, '$.country') AS country
                , JSON_VALUE(addresses_json, '$.primary') AS primary
                , JSON_VALUE(addresses_json, '$.street_address') AS street_address
            FROM UNNEST(JSON_QUERY_ARRAY(ADDRESSES)) AS addresses_json
        ) as `$add`
    ) AS addresses
```
