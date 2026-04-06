

-- {{ config(materialized='view') }}


-- select
--     sport_name,
--     count(distinct league_id) as most_leagues
-- from {{ source('raw_dataset', 'leagues') }}
-- group by sport_name
-- order by most_leagues desc
-- limit 10

-- with the renamed columns
{{ config(materialized='view') }}

select
    strSport as sport_name,
    count(distinct idLeague) as most_leagues
from {{ source('raw_dataset', 'leagues') }}
group by strSport
order by most_leagues desc
limit 10
