select
    cast(year as int) as year,
    cast(race_date as date) as race_date,

    cast(temp_6am as double) as temp_6am,
    cast(dew_6am as double) as dew_6am,
    cast(wind_6am as double) as wind_6am,

    cast(temp_9am as double) as temp_9am,
    cast(dew_9am as double) as dew_9am,
    cast(wind_9am as double) as wind_9am,

    cast(temp_12pm as double) as temp_12pm,
    cast(dew_12pm as double) as dew_12pm,
    cast(wind_12pm as double) as wind_12pm

from {{ source('marathon', 'bronze_weather') }}