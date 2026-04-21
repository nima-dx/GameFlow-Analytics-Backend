{{ config(materialized='view') }}

SELECT * FROM {{ source('raw', 'f1_race_results_2024') }}

UNION ALL

SELECT * FROM {{ source('raw', 'f1_race_results_2025') }}

UNION ALL

SELECT * FROM {{ source('raw', 'f1_race_results_2026') }}
