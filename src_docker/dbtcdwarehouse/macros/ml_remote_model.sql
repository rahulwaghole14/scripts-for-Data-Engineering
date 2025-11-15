{% macro gemini_remote_model_options(model) %}
    {% set options %}
        OPTIONS(
            endpoint = 'https://australia-southeast1-aiplatform.googleapis.com/v1/projects/{{ target.project }}/locations/australia-southeast1/publishers/google/models/{{ model }}:predict'
        )
    {% endset %}
    {{ return(options) }}
{% endmacro %}

{% macro create_gemini_remote_model_as(relation, model) %}
    create or replace model {{ relation }}
    REMOTE WITH CONNECTION `{{ target.project }}.australia-southeast1.gcf-tensorflow-connection`
    {{ gemini_remote_model_options(model) }}
{% endmacro %}

{% materialization gemini_remote_model, adapter='bigquery' %}
    {%- set identifier = model['alias'] -%}
    {%- set target_relation = api.Relation.create(database=database, schema=schema, identifier=identifier) -%}
    {%- set ml_config = config.get('ml_config', {}) -%}

    {{ run_hooks(pre_hooks) }}

    {% call statement('main') -%}
        {{ create_gemini_remote_model_as(
            target_relation,
            model=ml_config.get('model')
        ) }}
    {% endcall -%}

    {{ run_hooks(post_hooks) }}

    {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
