with qualifying as (

    select *
    from {{ ref('stg_qualifying_times') }}

),

expanded as (

    select
        gender,
        explode(
            case
                when chicago_age = 'Under 30' then sequence(19, 29)
                when chicago_age = '30-34' then sequence(30, 34)
                when chicago_age = '35-39' then sequence(35, 39)
                when chicago_age = '40-44' then sequence(40, 44)
                when chicago_age = '45-49' then sequence(45, 49)
                when chicago_age = '50-54' then sequence(50, 54)
                when chicago_age = '55-59' then sequence(55, 59)
                when chicago_age = '60-64' then sequence(60, 64)
                when chicago_age = '65-69' then sequence(65, 69)
                when chicago_age = '70-74' then sequence(70, 74)
                when chicago_age = '75-79' then sequence(75, 79)
                when chicago_age = '80 and Over' then sequence(80, 100)
            end
        ) as age,
        qualifying_time_2014,
        qualifying_time_2024,
        qualifying_time_2025

    from qualifying

),

final as (

    select
        gender,
        age,
        2014 as effective_start_year,
        2017 as effective_end_year,
        qualifying_time_2014 as qualifying_time_seconds
    from expanded

    union all

    select
        gender,
        age,
        2018 as effective_start_year,
        2024 as effective_end_year,
        qualifying_time_2024 as qualifying_time_seconds
    from expanded

    union all

    select
        gender,
        age,
        2025 as effective_start_year,
        9999 as effective_end_year,
        qualifying_time_2025 as qualifying_time_seconds
    from expanded

)

select *
from final