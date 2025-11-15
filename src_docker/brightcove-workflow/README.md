## new data requirement

For the video and editorial dashboards we require the data from Brightcove to be loaded with the Destination Path and Hourly View data.

### dest path
Destination path Dimensions and Fields are

```py
dimensions = ["date", "destination_path"]
fields = [
    "video_view",
    "video_impression",
    "video_percent_viewed",
    "video_seconds_viewed",
    "ad_mode_begin",
    "ad_mode_complete",
]

# plus brightcove_account, record_load_dts
```

### hourly
and for Hourly Views it is

```py
dimensions = ["date", "date_hour"]
fields = [
    "video_view",
    "video_impression",
    "video_percent_viewed",
    "video_seconds_viewed"
    ]
```

Both also have the Account Name added as a column, and Destination Path has a record_load_dts field.

There are existing tables in BQ in the magic_metrics data set that can be copied across as a base table to cdw_stage, if possible these should be partitioned by date.

`magic_metrics.brightcove__daily_videos_destination`
`magic_metrics.brightcove__hourly_video`

Data to be uploaded daily (for previous day) early AM, as with the standard Brightcove dataset.
