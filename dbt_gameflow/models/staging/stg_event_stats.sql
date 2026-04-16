{{ config(materialized='view') }}

select
  *
from
  {{ source('raw', 'event_stats') }}
