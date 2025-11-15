# Google Analytics Data Source Documentation

Author: [David Powell](mailto:david.powell@hexa.co.nz)

---

## Table of Contents

1. [About Google Analytics](#about-google-analytics)
2. [Data Export Options and Resources](#data-export-options-and-resources)
3. [hexa Properties](#hexa-properties)

---

## About Google Analytics

Google Analytics (GA) is a web tracking tool that provides session information (hit data). Universal Analytics (UA) and GA4 are different implementations with distinct schemas.

- [UA Support Features and Costs Comparison](https://support.google.com/analytics/answer/3437618?hl=en&ref_topic=3416089&sjid=696958948691552645-AP#zippy=%2Ccompare-bigquery-export-in-google-analytics-and-universal-analytics)
- [UA Schema](https://support.google.com/analytics/answer/3437719?hl=en) (only available for 360 customers)
- [GA4 Schema](https://support.google.com/analytics/answer/7029846?hl=en)

### UA Schema Business/Combination Key
When Google Analytics 360 data is exported to BigQuery, each row of data is associated with a unique combination of identifiers that serve as a compound primary key. This means that there's not a single "primary key" but rather a combination of fields that, together, uniquely identify each row of data.

The typical primary key in this context is a combination of these fields:

fullVisitorId: This ID is a unique identifier associated with a particular visitor's device or browser.
visitId: The visitId represents a specific session by the user. This is unique for each visit by the same user.
visitStartTime: The timestamp (in POSIX time) of the visit's start time.
Keep in mind that while this combination will typically provide a unique identifier for each session, there can be edge cases where it doesn't. For example, if a user opens multiple tabs to the same site simultaneously, it could lead to the same combination of fullVisitorId, visitId, and visitStartTime.

### GA4 Schema Business/Combination Key
In Google Analytics 4 (GA4), the concept of a session is not as explicitly defined as it was in Universal Analytics. GA4 focuses more on event-based tracking rather than session-based tracking. That said, you can still analyze sessions in GA4, but the keys to define a unique session aren't as straightforward.

A session in GA4 is recognized by an event_name of session_start. To uniquely identify these sessions, you could potentially use a combination of user_pseudo_id (which identifies a unique user) and event_timestamp_micros (which provides the timestamp of the session start).


---

## Data Export Options and Resources

Google Analytics exports data through several methods:

- Bulk export to BigQuery (GA4 & 360 only)
- API access

## Sample Data for testing

- [Google Analytics sample dataset for BigQuery](https://support.google.com/analytics/answer/7586738?hl=en)
- [BigQuery sample dataset for Google Analytics ecommerce web implementation](https://developers.google.com/analytics/bigquery/web-ecommerce-demo-dataset?sjid=14759099738392044774-AP)


## dbt docs blocks

{% docs event_params %}

The `event_params` RECORD can store campaign-level and contextual event parameters as well as any user-defined event parameters. The `event_params` RECORD is repeated for each key that is associated with an event.

The set of parameters stored in the `event_params` RECORD is unique to each implementation. To see the complete list of event parameters for your implementation, query the event parameter list.

| Field name                     | Data type | Description                                                   |
|--------------------------------|-----------|---------------------------------------------------------------|
| event_params.key               | STRING    | The name of the event parameter.                              |
| event_params.value             | RECORD    | A record containing the event parameter's value.              |
| event_params.value.string_value| STRING    | If the event parameter is represented by a string, such as a URL or campaign name, it is populated in this field. |
| event_params.value.int_value   | INTEGER   | If the event parameter is represented by an integer, it is populated in this field. |
| event_params.value.double_value| FLOAT     | If the event parameter is represented by a double value, it is populated in this field. |
| event_params.value.float_value | FLOAT     | If the event parameter is represented by a floating point value, it is populated in this field. This field is not currently in use. |

{% enddocs %}

{% docs ga4_bigquery_export %}

This schema represents the Google Analytics 4 (GA4) property data and the Google Analytics for Firebase data that is exported to BigQuery.

**Datasets:**
For each Google Analytics 4 property and each Firebase project that is linked to BigQuery, a single dataset named "analytics_<property_id>" is added to your BigQuery project. Property ID refers to your Analytics Property ID, which you can find in the property settings for your Google Analytics 4 property, and in App Analytics Settings in Firebase. Each Google Analytics 4 property and each app for which BigQuery exporting is enabled will export its data to that single dataset.

**Tables:**
Within each dataset, a table named events_YYYYMMDD is created each day if the Daily export option is enabled.

If the Streaming export option is enabled, a table named events_intraday_YYYYMMDD is created. This table is populated continuously as events are recorded throughout the day. This table is deleted at the end of each day once events_YYYYMMDD is complete.

Not all devices on which events are triggered send their data to Analytics on the same day the events are triggered. To account for this latency, Analytics will update the daily tables (events_YYYYMMDD) with events for those dates for up to three days after the dates of the events. Events will have the correct timestamp regardless of arriving late. Events that arrive after that three-day window are not recorded.

If you are using BigQuery sandbox, there is no intraday import of events, and additional limits apply. Upgrade from the sandbox if you want intraday imports.

**Columns:**
Each column in the events_YYYYMMDD table represents an event-specific parameter. Note that some parameters are nested within RECORDS, and some RECORDS such as items and event_params are repeatable.

{% enddocs %}
