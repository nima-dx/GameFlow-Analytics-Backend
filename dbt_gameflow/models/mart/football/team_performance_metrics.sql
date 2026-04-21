{{ config(materialized='view') }}

with event_outlook as (
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
),
event_log as (
  select
    event_id,
    homeaway,
    Shots_on_Goal,
    Shots_outsidebox,
    Shots_insidebox,
    Goalkeeper_Saves,
    score,
    Blocked_Shots,
    Shots_off_Goal,
    Passes_accurate,
    Fouls,
    Total_passes,
    Passes_pct,
    Total_Shots,
    goals_prevented,
    Corner_Kicks,
    Offsides,
    Yellow_Cards,
    Red_Cards,
    expected_goals,
    points,
<<<<<<< HEAD
    [point],
    [point],
    [point],
=======
>>>>>>> main
    Ball_Possession
  from {{ ref('int_event_log') }}
)

select
    ev_lk.event_id,
    ev_lk.league_name,
    ev_lk.sport_name,
    ev_lk.homeaway,
    ev_lk.team_name,
    ev_lk.season,
    ev_lk.venue_name,
    ev_lk.country_name,
    sum(ev_lg.Shots_on_Goal) as Shots_on_Goal,
    sum(ev_lg.Shots_outsidebox) as Shots_outsidebox,
    sum(ev_lg.Shots_insidebox) as Shots_insidebox,
    sum(ev_lg.Goalkeeper_Saves) as Goalkeeper_Saves,
    sum(ev_lg.score) as score,
    sum(ev_lg.Blocked_Shots) as Blocked_Shots,
    sum(ev_lg.Shots_off_Goal) as Shots_off_Goal,
    sum(ev_lg.Passes_accurate) as Passes_accurate,
    sum(ev_lg.Fouls) as Fouls,
    sum(ev_lg.Total_passes) as Total_passes,
    sum(ev_lg.Passes_pct) as Passes_pct,
    sum(ev_lg.Total_Shots) as Total_Shots,
    sum(ev_lg.goals_prevented) as goals_prevented,
    sum(ev_lg.Corner_Kicks) as Corner_Kicks,
    sum(ev_lg.Offsides) as Offsides,
    sum(ev_lg.Yellow_Cards) as Yellow_Cards,
    sum(ev_lg.Red_Cards) as Red_Cards,
    sum(ev_lg.expected_goals) as expected_goals,
    sum(ev_lg.points) as points,
<<<<<<< HEAD
    sum(ev_lg.[point]) as point,
    sum(ev_lg.[point]) as point,
    sum(ev_lg.[point]) as point,
=======
>>>>>>> main
    sum(ev_lg.Ball_Possession) as Ball_Possession
from
    event_outlook ev_lk
    inner join event_log ev_lg
    on ev_lk.event_id = ev_lg.event_id
    and ev_lk.homeaway = ev_lg.homeaway
group by
    ev_lk.event_id,
    ev_lk.league_name,
    ev_lk.sport_name,
    ev_lk.homeaway,
    ev_lk.team_name,
    ev_lk.season,
    ev_lk.venue_name,
    ev_lk.country_name
