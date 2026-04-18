{{ config(materialized='view') }}

with players_timeline as (
  select
  event_id,
  homeaway,
  yellow_cards,
  red_cards,
  var,
  subst,
  normal_goals,
  penalties,
  own_goals,
  player_name,
  assist_name,
  minute
  from {{ ref('int_players_timeline') }}
),

event_outlook as (
  select
    event_id,
    league_name,
    sport_name,
    homeaway,
    team_name,
    season,
    venue_name,
    country_name
  from {{ ref('int_event_outlook') }}
)

select
    e.event_id,
    e.league_name,
    e.sport_name,
    e.homeaway,
    e.team_name,
    e.season,
    e.venue_name,
    e.country_name,
    pt.yellow_cards,
    pt.red_cards,
    pt.var,
    pt.subst,
    pt.normal_goals,
    pt.penalties,
    pt.own_goals,
    pt.player_name,
    pt.assist_name,
    pt.minute
from
  event_outlook e
  inner join players_timeline pt
  on e.event_id = pt.event_id  and e.homeaway = pt.homeaway
