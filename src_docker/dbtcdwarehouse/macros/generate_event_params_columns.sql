{% macro generate_event_params_columns(table_name, event_timestamp) %}
    {% set query %}
    SELECT DISTINCT ep.key AS event_param_key
    FROM {{ table_name }},
         UNNEST(event_params) AS ep
    WHERE event_timestamp = {{ event_timestamp }}
    {% endset %}

    {% set results = run_query(query).table %}
    {% set columns = [] %}

    {% for row in results %}
        {% set column_name = row.event_param_key.replace(' ', '_') | lower %}
        {% set string_column = "MAX(CASE WHEN ep.key = '" ~ row.event_param_key ~ "' THEN ep.value.string_value ELSE NULL END) AS " ~ column_name ~ "_string" %}
        {% set int_column = "MAX(CASE WHEN ep.key = '" ~ row.event_param_key ~ "' THEN ep.value.int_value ELSE NULL END) AS " ~ column_name ~ "_int" %}
        {% set double_column = "MAX(CASE WHEN ep.key = '" ~ row.event_param_key ~ "' THEN ep.value.double_value ELSE NULL END) AS " ~ column_name ~ "_double" %}
        {% do columns.append(string_column) %}
        {% do columns.append(int_column) %}
        {% do columns.append(double_column) %}
    {% endfor %}

    {{ return(columns | join(',\n    ')) }}
{% endmacro %}
