original handover doc from Deane
https://docs.google.com/document/d/13fIACbaW5p3iJNRIE3usJzdVBzzirXudnWRz7Dsr5aQ/edit

Jira issue to build this script
https://hexanz.atlassian.net/browse/CD-1004
feat/CD-1004-get-piano-conversion-data-files-into-bigquery
https://hexanz.atlassian.net/browse/CD-1002

file locations on share drive
Uira Datafile Location: \\pdcfilcl102\group$\FFX\Data and Insights\Power BI Datasets\Digital Mastheads\
Cut & Paste the file into the “Report Conversions\[masthead]” folder
Cut & Paste the file into the “Composer Conversions\[masthead]” folder


# Automation Documentation

run main script
```
cd src
python -m piano__conversions_to_bigquery.main
```

## Track work on this task

[CD-1003 Jira Ticket](https://hexanz.atlassian.net/browse/CD-1003)

## VX Conversion Report
VX is Piano's commerce engine, which handles everything from pricing and promotions to payment processing and subscription lifecycle management. When a visitor completes a desired action (like purchasing a subscription or redeeming a free trial), this is considered a VX conversion. It tracks both monetary and non-monetary conversions to give businesses detailed insights into their users' purchasing behavior and subscription health
https://piano.io/
https://support.piano.io/hc/en-us/articles/5491188783890-Data-Model-Structure
https://docs.piano.io/track/vx-overview/?chapter=2335#paymentterm

[Schedule VX Conversion Report Export - Piano API Documentation](https://docs.piano.io/api/?endpoint=post~2F~2Fexport~2Fschedule~2Fvx~2Fconversion)

### example request
```bash
curl -X POST --header 'Accept: application/json' 'https://reports-api.piano.io/rest/export/schedule/vx/conversion?aid={app_id}&api_token={app_api_token}&row_limit=500'
```
### example response
```json
{
  "export_id": "{export_id}",
  "report_request": {
    "aid": "{app_id}",
    "start": "2024-01-15",
    "end": "2024-02-14",
    "file_name": "conversion-all-01-15-2024--02-14-2024.zip",
    "row_limit": 500
  },
  "job_status": "CREATED"
}
```

**Method:** `POST`
**Path:** `/export/schedule/vx/conversion`

### Parameters

| Parameter    | Value          | Description                                                          | Type    |
| ------------ | -------------- | -------------------------------------------------------------------- | ------- |
| `aid`        | (required)     | The application ID                                                   | string  |
| `row_limit`  | 500            | Maximum row count in the by-page aggregation. Not limited if set to 0.| integer |
| `offer_id`   |                | The offer ID                                                         | string  |
| `term_id`    |                | The public term ID to filter by                                      | string  |
| `currency`   |                | The term currency                                                    | string  |
| `term_type`  |                | The term type to filter by                                           | array   |
| `checkout_flow_id` |          | The public checkout ID                                               | string  |
| `from`       |                | The report start date in the application time zone (yyyy-MM-dd)      | string  |
| `to`         |                | The report end date in the application time zone (yyyy-MM-dd)        | string  |
| `file_name`  |                | The name of the generated file without an extension                  | string  |

### Response Messages

- **200:** Successful operation
- **400:** Invalid parameters
- **500:** Internal exception

### Response Class

```java
ReportExportJobInfo {
    export_id (string): Unique export ID
    report_request (object): The report request params, a JSON string
    job_status (string): The job status
    error_text (string): The error message when the Job status is INTERNAL_ERROR
    percent_complete (int32): The percent complete if applicable
}
```

## Composer Conversion Report

[Schedule Composer Conversion Report Export - Piano API Documentation](https://docs.piano.io/api?endpoint=post~2F~2Fexport~2Fschedule~2Fcomposer~2Fconversion)

**Method:** `POST`
**Path:** `/export/schedule/composer/conversion`

### Parameters

| Parameter     | Value          | Description                                                          | Type    |
| ------------- | -------------- | -------------------------------------------------------------------- | ------- |
| `aid`         | (required)     | The application ID                                                   | string  |
| `exp_id`      | (required)     | The experience ID                                                    | string  |
| `version_num` |                | The version number                                                   | string  |
| `row_limit`   | 10000          | Maximum row count in the by-page aggregation. Not limited if set to 0.| integer |
| `from`        |                | The report start date in the application time zone (yyyy-MM-dd)      | string  |
| `to`          |                | The report end date in the application time zone (yyyy-MM-dd)        | string  |
| `file_name`   |                | The name of the generated file without an extension                  | string  |

### Response Messages

- **200:** Successful operation
- **400:** Invalid parameters
- **500:** Internal exception

### Response Class

```java
ReportExportJobInfo {
    export_id  (string) : Unique export ID
    report_request  (object) : The report request params, a JSON string
    job_status  (string) : The job status
    error_text  (string) : The error message when the Job status is INTERNAL_ERROR
    percent_complete  (int32) : The percent complete if applicable
}
```

## Ping Status of Report Export

### example request
```bash
curl -X GET --header 'Accept: application/json' 'https://reports-api.piano.io/rest/export/status?aid={app_id}&api_token={app_api_token}&export_id={export_id}'
```

### example response
```json
{
  "export_id": "{export_id}",
  "report_request": {
    "aid": "{app_id}",
    "start": "2024-01-15",
    "end": "2024-02-14",
    "file_name": "conversion-all-01-15-2024--02-14-2024.zip",
    "row_limit": 500
  },
  "job_status": "FINISHED"
}
```

[Get Status of Report Export Job - Piano API Documentation](https://docs.piano.io/api?endpoint=get~2F~2Fexport~2Fstatus)

**Method:** `GET`
**Path:** `/export/status`

### Parameters

| Parameter   | Value          | Description                         | Type    |
| ----------- | -------------- | ----------------------------------- | ------- |
| `aid`       | (required)     | The application ID                  | string  |
| `export_id` | (required)     | The ID of the exported report       | string  |

### Response Messages

- **200:** Successful operation
- **400:** Invalid export_id
- **500:** Internal exception

### Response Class

```java
ReportExportJobInfo {
    export_id  (string) : Unique export ID
    report_request  (object) : The report request params, a JSON string
    job_status  (string) : The job status
    error_text  (string) : The error message when the Job status is INTERNAL_ERROR
    percent_complete  (int32) : The percent complete if applicable
}
```

## Download Report

### example request
```bash
curl -X GET --header 'Accept: application/json' 'https://reports-api.piano.io/rest/export/status?aid={app_id}&api_token={app_api_token}&export_id={export_id}'
```

### example response
```json
{
  "export_id": "{export_id}",
  "report_request": {
    "aid": "{app_id}",
    "start": "2024-01-15",
    "end": "2024-02-14",
    "file_name": "conversion-all-01-15-2024--02-14-2024.zip",
    "row_limit": 500
  },
  "job_status": "FINISHED"
}
```

[Get URL to Download Report Export - Piano API Documentation](https://docs.piano.io/api?endpoint=get~2F~2Fexport~2Fdownload~2Furl)

**Method:** `GET`
**Path:** `/export/download/url`

### Parameters

| Parameter   | Value          | Description                  | Type    |
| ----------- | -------------- | ---------------------------- | ------- |
| `aid`       | (required)     | The application ID           | string  |
| `export_id` | (required)     | Unique export ID             | string  |

### Response Messages

- **200:** Successful operation
- **400:** Invalid export_id or the job is not finished
- **500:** Internal exception

### Response Class

```java
DownloadUrl {
    url  (string) : The download URL
}
