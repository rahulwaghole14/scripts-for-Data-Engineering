feat/CD-1004-get-piano-conversion-data-files-into-bigquery

I think that we're using email in the first instance. Then the user is given UID.
I don't think UID will match any records on our end. Email will be the unique identifier.

Each masthead has its own AID (app id). Each masthead has its own user repository. Each user will have their unique UID (user id).

When a user subscribes to eg The Post,
that's where their subscription details will be stored ie within that app.
But, I believe, that when they access another masthead site with their All Access subscription,
for which they need to log in. During the login process (as part of JWT token exchange)
a user will be created on each masthead instance as well.

https://hexanz.atlassian.net/wiki/spaces/SRIJ/pages/2695364614/Matrix+Piano+Entitlement+Mapping

https://docs.piano.io/api/

# List Subscriptions

This endpoint lists the subscriptions of a given application, optionally applying filters. Note that `User.custom_fields` are not populated for this endpoint.

## HTTP Method: GET
### Endpoint
```
PATH/publisher/subscription/list
```


### Parameters

| Parameter    | Value       | Description                                                                                                                        | Type    |
|--------------|-------------|------------------------------------------------------------------------------------------------------------------------------------|---------|
| `aid`        | (required)  | The application ID.                                                                                                                | string  |
| `uid`        |             | The user ID.                                                                                                                       | string  |
| `type`       |             | The type of subscription.                                                                                                          | string  |
| `start_date` |             | The start date for filtering. Without `select_by`, filters by "update date". To specify the filter field, use `select_by`.         | string  |
| `end_date`   |             | The end date for filtering. Without `select_by`, filters by "update date". To specify the filter field, use `select_by`.           | string  |
| `q`          |             | Search value.                                                                                                                      | string  |
| `offset`     | (required)  | Offset from which to start returning results.                                                                                      | integer |
| `limit`      | 100         | Maximum index of returned results. Default is 100.                                                                                 | integer |
| `select_by`  |             | Filter subscription date field.                                                                                                    | string  |
| `status`     |             | Subscription status.                                                                                                               | string  |

### Notes

- The `aid` parameter is required for all requests.
- The `offset` parameter is required and specifies the starting point from which to return results.
- The `limit` parameter is set to 100 by default, indicating the maximum number of results that can be returned.
- If using `start_date` and `end_date` for date-range filtering without specifying `select_by`, the method defaults to filtering by "update date". Use `select_by` to explicitly define the filter field.
- The `status` parameter allows filtering subscriptions based on their status.
- Response classes can be documented in `validator.py`
