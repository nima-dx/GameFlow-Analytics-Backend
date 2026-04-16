
-- Event Lookup
select distinct
  ds.event_id,
  ds.league_name,
  ds.sport_name,
  ds.HomeAway,
  ds.Team_name,
  ev_tl.season,
  ds.venue_name,
  ds.country_name
from
(
select
  ev.event_id,
  ev.league_name,
  ev.sport_name,
  'Home' as HomeAway,
  ev.home_team_name as Team_name,
  ev.venue_name,
  ev.country_name
from
    `le-wagon-data-atelier.raw_dataset.events` ev

union all

select
  ev.event_id,
  ev.league_name,
  ev.sport_name,
  'Away' as HomeAway,
  ev.away_team_name as Team_name,
  ev.venue_name,
  ev.country_name
from
    `le-wagon-data-atelier.raw_dataset.events` ev
) ds
    inner join `le-wagon-data-atelier.raw_dataset.event_timeline` ev_tl
    on ds.event_id = ev_tl.event_id


--------->>>>>>


select
  event_id,
  'home' as homeaway,
  stat_name,
  home_value as value
from
    `le-wagon-data-atelier.raw_dataset.event_stats`


select
  event_id,
  'away' as homeaway,
  stat_name,
  away_value as value
from
    `le-wagon-data-atelier.raw_dataset.event_stats`


select
  event_id,
  'home' as homeaway,
  'score' as stat_name
  home_score as value
from
`le-wagon-data-atelier.raw_dataset.events`


select
  event_id,
  'away' as homeaway,
  'score' as stat_name
  away_score as value
from
`le-wagon-data-atelier.raw_dataset.events`


select
  event_id,
  'home' as homeaway,
  'point' as stat_name,
  case when home_score > away_score then 3
        when home_score = away_score then 1
        when home_score < away_score then 0
  end as value
from
`le-wagon-data-atelier.raw_dataset.events`

select
  event_id,
  'away' as homeaway,
  'point' as stat_name,
  case when home_score > away_score then 0
        when home_score = away_score then 1
        when home_score < away_score then 3
  end as value
from
`le-wagon-data-atelier.raw_dataset.events`
