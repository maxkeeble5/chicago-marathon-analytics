with results as (

    select *
    from {{ ref('stg_results') }}

),

weather as (

    select *
    from {{ ref('stg_weather') }}

),

final as (

    select
        r.year,
        r.place_overall,
        r.place_gender,
        r.bib,
        r.athlete_name,
        r.athlete_id,
        r.gender,
        r.age_group,
        r.country_ioc,
        r.is_usa,
        r.is_brazil,
        r.finish_time,
        r.finish_time_seconds,
        r.half_split_seconds,
        r.decade,
        r.is_post_major,

        w.race_date,
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

    left join weather w
        on r.year = w.year

)

select *
from final