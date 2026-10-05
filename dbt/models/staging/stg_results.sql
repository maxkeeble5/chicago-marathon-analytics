select
    race,
    cast(year as int) as year,
    name,
    gender,
    cast(age as int) as age,
    country,
    cast(overall as int) as overall,
    finish_time,
    cast(finish as int) as finish
from {{ source('marathon', 'bronze_results') }}