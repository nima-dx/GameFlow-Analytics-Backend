{{ config(materialized='view') }}

SELECT
  strStatus,
  strVenue,
  dateEventLocal,
  strTime,
  strDescriptionEN,
  strTimestamp,
  strCountry,
  dateEvent,
  strCity,
  strEvent
FROM {{ source('raw', 'f1_calendar_2026') }}

UNION ALL

SELECT
  strStatus,
  strVenue,
  dateEventLocal,
  strTime,
  strDescriptionEN,
  strTimestamp,
  strCountry,
  dateEvent,
  strCity,
  strEvent
FROM {{ source('raw', 'f1_calendar_2025') }}

UNION ALL

SELECT
  strStatus,
  strVenue,
  dateEventLocal,
  strTime,
  strDescriptionEN,
  strTimestamp,
  strCountry,
  dateEvent,
  strCity,
  strEvent
FROM {{ source('raw', 'f1_calendar_2024') }}
