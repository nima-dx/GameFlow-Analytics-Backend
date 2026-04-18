
with cte_home_eventlog as (
  select
    event_id,
    'home' as homeaway,
    stat_name,
    home_value as value
  from
      {{ ref('stg_event_stats') }}
),
cte_away_eventlog as (
  select
    event_id,
    'away' as homeaway,
    stat_name,
    away_value as value
  from
      {{ ref('stg_event_stats') }}
),
cte_home_scorelog as (
  select
    event_id,
    'home' as homeaway,
    'score' as stat_name,
    home_score as value
  from
      {{ ref('stg_events') }}
),
cte_away_scorelog as (
  select
    event_id,
    'away' as homeaway,
    'score' as stat_name,
    away_score as value
  from
      {{ ref('stg_events') }}
),
cte_home_pointlog as (
  select
    event_id,
    'home' as homeaway,
    'point' as stat_name,
    case when home_score > away_score then 3
          when home_score = away_score then 1
          when home_score < away_score then 0
    end as value
  from
      {{ ref('stg_events') }}
),
cte_away_pointlog as (
  select
    event_id,
    'away' as homeaway,
    'point' as stat_name,
    case when home_score > away_score then 0
          when home_score = away_score then 1
          when home_score < away_score then 3
    end as value
  from
      {{ ref('stg_events') }}
)


select
  event_id,
  homeaway,
  sum(case when stat_name = 'Shots on Goal' then value end) as Shots_on_Goal,
  sum(case when stat_name = 'Shots outsidebox' then value end) as Shots_outsidebox,
  sum(case when stat_name = 'Shots insidebox' then value end) as Shots_insidebox,
  sum(case when stat_name = 'Goalkeeper Saves' then value end) as Goalkeeper_Saves,
  sum(case when stat_name = 'score' then value end) as score,
  sum(case when stat_name = 'Blocked Shots' then value end) as Blocked_Shots,
  sum(case when stat_name = 'Shots off Goal' then value end) as Shots_off_Goal,
  sum(case when stat_name = 'Passes accurate' then value end) as Passes_accurate,
  sum(case when stat_name = 'Fouls' then value end) as Fouls,
  sum(case when stat_name = 'Total passes' then value end) as Total_passes,
  sum(case when stat_name = 'Passes %' then value end) as Passes_pct,
  sum(case when stat_name = 'Total Shots' then value end) as Total_Shots,
  sum(case when stat_name = 'goals_prevented' then value end) as goals_prevented,
  sum(case when stat_name = 'Corner Kicks' then value end) as Corner_Kicks,
  sum(case when stat_name = 'Offsides' then value end) as Offsides,
  sum(case when stat_name = 'Yellow Cards' then value end) as Yellow_Cards,
  sum(case when stat_name = 'Red Cards' then value end) as Red_Cards,
  sum(case when stat_name = 'expected_goals' then value end) as expected_goals,
  sum(case when stat_name = 'point' then value end) as points,
  sum(case when stat_name = 'Ball Possession' then value end) as Ball_Possession
from
(
  select * from cte_home_eventlog
  union all
  select * from cte_away_eventlog
  union all
  select * from cte_home_scorelog
  union all
  select * from cte_away_scorelog
  union all
  select * from cte_home_pointlog
  union all
  select * from cte_away_pointlog
)uni
group by
  event_id,
  homeaway
