select
    gender,
    chicago_age,
    cast(`2025_cq` as int) as qualifying_time_2025,
    cast(`2024_cq` as int) as qualifying_time_2024,
    cast(`2014_cq` as int) as qualifying_time_2014

from {{ source('marathon', 'bronze_qualifying_times') }}