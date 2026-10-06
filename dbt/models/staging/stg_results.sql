select
    cast(year as int) as year,
    cast(place_overall as int) as place_overall,
    cast(place_gender as int) as place_gender,
    bib,
    athlete_name,
    athlete_id,
    gender,
    age_group,
    country_ioc,
    is_usa,
    is_brazil,
    finish_time,
    cast(finish_time_seconds as double) as finish_time_seconds,
    cast(half_split_seconds as double) as half_split_seconds,
    decade,
    is_post_major
from {{ source('marathon', 'bronze_results') }}