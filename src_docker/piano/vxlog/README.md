Schedule VX Subscription log export
https://docs.piano.io/api/?endpoint=post~2F~2Fexport~2Fschedule~2Fvx~2FsubscriptionLog

Schedules a job exporting a given VX Subscription log.

METHODPOST
PATH/export/schedule/vx/subscriptionLog
Parameters
Parameter	Value	Description	Type
aid
(required)
Application ID	string

response

```java
ReportExportJobInfo
{
    export_id  (string) : Unique export ID
    report_request  (object) : The report request params, a JSON string
    job_status  (string) : The job status
    error_text  (string) : The error message when the Job status is INTERNAL_ERROR
    percent_complete  (int32) : The percent complete if applicable
}
```



Get URL to download report export

Returns a JSON object which contains a link to download the report. The link is temporary and has to be regenerated after 30 minutes.

METHODGET
PATH/export/download/url

Parameters
Parameter	Value	Description	Type
aid
(required)
The application ID	string
export_id
(required)
Unique export ID	string
