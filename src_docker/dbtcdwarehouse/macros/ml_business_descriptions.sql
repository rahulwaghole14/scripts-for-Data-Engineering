-- macros/classify_text.sql
{% macro generate_business_description(model_ref, input_table, text_column, name_id_column, company_name_column, brand_id_column, brand_name_column) %}

    (
        SELECT
            {{ name_id_column }},
            {{ company_name_column }},
            {{ brand_id_column }},
            {{ brand_name_column }},
            ml_generate_text_result.ml_generate_text_result AS classification
        FROM
            ML.GENERATE_TEXT(
                MODEL {{ model_ref }},
                (
                    SELECT
                        {{ name_id_column }},
                        {{ company_name_column }},
                        {{ brand_id_column }},
                        {{ brand_name_column }},
                        CONCAT(
                            'Create a concise business description for ',
                            {{ text_column }},
                            ', highlighting its primary activities/function and target market (this is probably a New Zealand business)'
                        ) AS prompt
                    FROM
                        {{ input_table }}
                ),
                STRUCT(
                    0.2 AS temperature,
                    100 AS max_output_tokens,
                    0.1 AS top_p,
                    10 AS top_k
                )
            ) AS ml_generate_text_result
    )
{% endmacro %}
