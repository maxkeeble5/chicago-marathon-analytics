with results as (

    select *
    from {{ ref('stg_results') }}

),

qualifying_times as (

    select *
    from {{ ref('int_qualifying_times_by_age') }}

),

weather as (

    select *
    from {{ ref('stg_weather') }}

),

final as (

    select
        r.race,
        r.year,
        r.name,
        r.gender,
        r.age,
        r.country,
        r.overall,
        r.finish_time,
        r.finish as finish_seconds,

        q.qualifying_time_seconds,

        case
            when q.qualifying_time_seconds is null then null
            when r.finish <= q.qualifying_time_seconds then true
            else false
        end as met_qualifying_standard,

        w.date as race_date,
        w.temp_6am,
        w.dew_6am,
        w.wind_6am,
        w.temp_9am,
        w.dew_9am,
        w.wind_9am,
        w.temp_12pm,
        w.dew_12pm,
        w.wind_12pm

    from results r

    left join qualifying_times q
        on r.gender = q.gender
        and r.age = q.age
        and r.year between q.effective_start_year and q.effective_end_year

    left join weather w
        on r.year = w.year

)

select *
from final