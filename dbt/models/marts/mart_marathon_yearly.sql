with runner_results as (

    select *
    from {{ ref('mart_runner_results') }}

),

final as (

    select
        year,
        race_date,

        count(*) as runner_count,

        avg(age) as average_age,

        sum(case when gender = 'M' then 1 else 0 end) as male_runner_count,
        sum(case when gender = 'F' then 1 else 0 end) as female_runner_count,

        avg(finish_seconds) as average_finish_seconds,
        percentile_approx(finish_seconds, 0.5) as median_finish_seconds,

        sum(
            case when met_qualifying_standard = true then 1 else 0 end
        ) as qualifying_runner_count,

        temp_6am,
        dew_6am,
        wind_6am,
        temp_9am,
        dew_9am,
        wind_9am,
        temp_12pm,
        dew_12pm,
        wind_12pm

    from runner_results

    group by
        year,
        race_date,
        temp_6am,
        dew_6am,
        wind_6am,
        temp_9am,
        dew_9am,
        wind_9am,
        temp_12pm,
        dew_12pm,
        wind_12pm

)

select *
from final