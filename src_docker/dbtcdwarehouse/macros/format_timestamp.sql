{% macro format_timestamp(datetime) %}
    CASE
        WHEN {{ datetime }} IS NULL THEN NULL
        WHEN regexp_contains(
            {{ datetime }},
            r'\+\d{4}$'
        ) THEN STRUCT(
            format_timestamp(
                '%Y-%m-%dT%H:%M:%S%Ez',
                parse_timestamp(
                    '%Y-%m-%d %H:%M:%S %z',
                    {{ datetime }}
                ),
                'UTC'
            ) AS `$time`
        )
        WHEN regexp_contains(
            {{ datetime }},
            r'^\d{4}-\d{2}-\d{2}$'
        ) THEN STRUCT(
            format_timestamp(
                '%Y-%m-%dT%H:%M:%S%Ez',
                parse_timestamp(
                    '%Y-%m-%d',
                    {{ datetime }}
                ),
                'UTC'
            ) AS `$time`
        )
        ELSE STRUCT(
            format_timestamp(
                '%Y-%m-%dT%H:%M:%S%Ez',
                parse_timestamp(
                    '%Y-%m-%d %H:%M:%S',
                    {{ datetime }}
                ),
                'UTC'
            ) AS `$time`
        )
    END
{% endmacro %}
