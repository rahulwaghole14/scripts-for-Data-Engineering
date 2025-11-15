{% macro generate_uuid_v5(reference, namespace=None) %}
    {% if namespace is not none and namespace|length > 0 %}
        {% set uuid_namespace = namespace %}
    {% else %}
        {% set uuid_namespace = env_var('UUID_NAMESPACE') %}
    {% endif %}
    {% set namespace_in_bytes = 'FROM_HEX(uuid_namespace)' %}
    TO_HEX(
        CAST(
            TO_HEX(
                LEFT(
                    SHA1(
                        CONCAT(
                            FROM_HEX(REPLACE('{{ uuid_namespace }}', '-', '')),
                            CAST({{ reference }} AS BYTES FORMAT 'UTF8'))), 16)) AS BYTES FORMAT 'HEX') & CAST('ffffffffffff0fff3fffffffffffffff' AS BYTES FORMAT 'HEX') | CAST('00000000000050008000000000000000'
            AS BYTES FORMAT 'HEX')
    )
{% endmacro %}
