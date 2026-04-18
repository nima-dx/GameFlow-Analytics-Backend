{{ config(materialized='view') }}

select
  *
from
  {{ source('raw', 'seasons') }}
