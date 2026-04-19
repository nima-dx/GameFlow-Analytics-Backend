{{ config(materialized='view') }}

SELECT
  strStatus,
  strVenue,
  dateEventLocal,
  strTime,
  strDescriptionEN
  strVenue,
  strTimestamp,
  strCountry,
  dateEvent,
  strCity,
  strEvent,
FROM  {{ source('raw', 'f1_calendar_2026') }}
