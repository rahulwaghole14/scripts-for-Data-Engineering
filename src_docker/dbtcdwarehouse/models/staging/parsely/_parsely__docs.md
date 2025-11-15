# parsely
parsely? article tag data, it looks at hexa articles and creates automatic tags i.e. 'mcdonalds', 'bigmac'

# schema table
- parsely	tags

# alteryx workflow

*update* new script created here: https://github.com/hexaNZ/hexa-data-alteryx-workflows/tree/main/src/parsely__api_to_bigquery

we copy the data to bigquery using endpoint = "analytics/posts" api docs: https://docs.parse.ly/api-analytics-endpoint/

period_start - Start of date range to consider traffic from; see Date/Time Handling for formatting details. Defaults to 3 days ago, limited to most recent 90 days.
period_end - End of date range to consider traffic from; see Date/Time Handling for formatting details. Defaults to current date and time if not specified.

tag types:
1. site_tag: "parsely_smart"
2. high_level_smart_tag: "parsely_smart:iab:"
3. low_level_smart_tags: "parsely_smart:entity:"
