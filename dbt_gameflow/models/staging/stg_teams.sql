{{ config(materialized='view') }}

select
  *
from
  {{ source('raw', 'teams') }}
