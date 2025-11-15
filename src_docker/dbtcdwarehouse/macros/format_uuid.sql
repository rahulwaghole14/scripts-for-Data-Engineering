{% macro format_uuid(uuid) %}
    CONCAT(
        SUBSTR({{ uuid }}, 1, 8),
        "-",
        SUBSTR({{ uuid }}, 9, 4),
        "-",
        SUBSTR({{ uuid }}, 13, 4),
        "-",
        SUBSTR({{ uuid }}, 17, 4),
        "-",
        SUBSTR({{ uuid }}, 21, 12)
    )
{% endmacro %}
