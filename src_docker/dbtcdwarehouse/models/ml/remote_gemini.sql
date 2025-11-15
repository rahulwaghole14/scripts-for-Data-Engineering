{{ config(
    tags=['ml'],
    materialized='gemini_remote_model',
    ml_config={
        'model': 'gemini-1.5-pro'
    }
) }}

-- This SQL won't be used for remote models, but dbt requires some SQL here
SELECT 1 as dummy
