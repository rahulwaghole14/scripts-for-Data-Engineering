from google.cloud import bigquery


def create_bigquery_schema(schema_mapping):
    """
    Create a bigquery schema from a schema mapping
    """
    schema = []
    for key, value in schema_mapping.items():
        schema.append(
            bigquery.SchemaField(
                value["target"], value["type"], mode="REQUIRED"
            )
        )

    return schema
