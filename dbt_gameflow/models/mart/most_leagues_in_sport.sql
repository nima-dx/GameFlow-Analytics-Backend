
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

select
    sport_name,
    count(distinct league_id) as most_leagues
from {{ source('raw_dataset', 'leagues') }}
group by sport_name
order by most_leagues desc
limit 10

/*
    Uncomment the line below to remove records with null `id` values
*/
