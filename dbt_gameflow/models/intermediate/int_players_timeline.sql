{{ config(materialized='view') }}

with cte_players_timeline as (
  select
    event_id,
    case when is_home = 'Yes' then 'home'
        when is_home = 'No' then 'away'
    end as homeaway,
    timeline_type,
    timeline_detail,
    player_name,
    assist_name,
    minute
    from
      {{ ref('stg_event_timeline') }}
)


select
  event_id,
  homeaway,
  sum(case when timeline_type = 'Card' and timeline_detail = 'Yellow Card' then 1 else 0 end) as yellow_cards,
  sum(case when timeline_type = 'Card' and timeline_detail = 'Red Card' then 1 else 0 end) as red_cards,
  sum(case when timeline_type = 'Var' then 1 else 0 end) as var,
  sum(case when timeline_type = 'subst' then 1 else 0 end) as subst,
  sum(case when timeline_type = 'Goal' and timeline_detail = 'Normal Goal' then 1 else 0 end) as normal_goals,
  sum(case when timeline_type = 'Goal' and timeline_detail = 'Penalty' then 1 else 0 end) as penalties,
  sum(case when timeline_type = 'Goal' and timeline_detail = 'Own Goal' then 1 else 0 end) as own_goals,
  player_name,
  assist_name,
  minute
from
  cte_players_timeline
group by
  event_id,
  homeaway,
  player_name,
  assist_name,
  minute
