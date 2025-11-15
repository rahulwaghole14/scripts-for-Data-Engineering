-- macros/classify_text.sql
{% macro classify_text_with_model(model_ref, input_table, text_column, id_column) %}
    (
        SELECT
            {{ id_column }},
            ml_generate_text_result.ml_generate_text_result AS classification
        FROM
            ML.GENERATE_TEXT(
                MODEL {{ model_ref }},
                (
                    SELECT
                        {{ id_column }},
                        CONCAT(
                            {# 'Classify the user need of the following text into one of the categories: "inspire me", "update me", "divert me". Text: ', #}
                            'Classify the user need of the following text into one of the categories: "update me (provide the latest news)", "keep me engaged (entertain me)", "educate me (teach me something new)", "give me perspective (offer insights or opinions)", "divert me (distract me with something fun)", "inspire me (motivate me)", "help me (assist me with something)", "connect me (help me connect with others)". Text: ',
                            {{ text_column }},
                            ' User Need:'
                        ) AS prompt
                    FROM
                        {{ input_table }}
                ),
                STRUCT(
                    0.1 AS temperature,
                    100 AS max_output_tokens,
                    0.1 AS top_p,
                    10 AS top_k
                )
            ) AS ml_generate_text_result
    )
{% endmacro %}
