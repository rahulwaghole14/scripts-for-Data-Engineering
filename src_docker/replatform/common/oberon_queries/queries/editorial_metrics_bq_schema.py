from google.cloud import bigquery

editoral_metrics_bq_schema=[
        # Specify the type of columns whose type cannot be auto-detected. For
        # example the "title" column uses pandas dtype "object", so its
        # data type is ambiguous.
        bigquery.SchemaField("date", bigquery.enums.SqlTypeNames.DATE)
        # Indexes are written if included in the schema by name.
    ]

logged_in_users_bq_schema=[
        # Specify the type of columns whose type cannot be auto-detected. For
        # example the "title" column uses pandas dtype "object", so its
        # data type is ambiguous.
        bigquery.SchemaField("date", bigquery.enums.SqlTypeNames.DATE)
        # Indexes are written if included in the schema by name.
    ]

logged_in_user_detailed_bq_schema=[
        # Specify the type of columns whose type cannot be auto-detected. For
        # example the "title" column uses pandas dtype "object", so its
        # data type is ambiguous.
        bigquery.SchemaField("date", bigquery.enums.SqlTypeNames.DATE),
        bigquery.SchemaField("min date", bigquery.enums.SqlTypeNames.DATE)
        # Indexes are written if included in the schema by name.
    ]


