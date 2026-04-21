{{ config(materialized='view') }}

with cte_home_events as (
  select
    event_id,
    league_name,
    sport_name,
    'home' as homeaway,
    home_team_name as team_name,
    venue_name,
    country_name
  from
      {{ ref('stg_events') }}
),
cte_away_events as (
  select
    event_id,
    league_name,
    sport_name,
    'away' as homeaway,
    away_team_name as team_name,
    venue_name,
    country_name
  from
      {{ ref('stg_events') }}
),
<<<<<<< HEAD
    ev.event_id,
    ev.league_name,
    ev.sport_name,
    'Home' as homeaway,
    ev.home_team_name as team_name,
    ev.venue_name,
    ev.country_name
  from
      {{ ref('stg_events') }}
),

cte_away_events as (
  select
    ev.event_id,
    ev.league_name,
    ev.sport_name,
    'Away' as homeaway,
    ev.away_team_name as team_name,
    ev.venue_name,
    ev.country_name
  from
      {{ ref('stg_events') }}
)
=======
>>>>>>> main

cte_timeline as (
  select
    *
  from
      {{ ref('stg_event_timeline') }}
)
<<<<<<< HEAD
),
),
),
=======
>>>>>>> main

  select distinct
    ds.event_id,
    ds.league_name,
    ds.sport_name,
    ds.homeaway,
    ds.team_name,
    ev_tl.season,
    ds.venue_name,
    ds.country_name
  from
  (
    select *
      from
    cte_home_events
    union all
    select *
      from
    cte_away_events
  )ds
    inner join cte_timeline ev_tl
    on ds.event_id = ev_tl.event_id
