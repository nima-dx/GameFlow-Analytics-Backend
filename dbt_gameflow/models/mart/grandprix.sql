{{ config(materialized='view') }}

select
    strSeason,
    strEvent,
    dateEvent,
    strTime,
    case
        when strStatus = 'Match Finished' then 'Completed'
        else 'Upcoming'
    end as RaceStatus

from {{ source('raw_dataset', 'grandprix') }}

order by dateEvent asc
