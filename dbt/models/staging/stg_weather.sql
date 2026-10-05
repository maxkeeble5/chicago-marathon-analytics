select
    cast(year as int) as year,
    cast(month as int) as month,
    cast(day as int) as day,
    cast(date as date) as date,

    cast(`6am_temp` as double) as temp_6am,
    cast(`6am_dew` as double) as dew_6am,
    cast(`6am_wind` as double) as wind_6am,

    cast(`9am_temp` as double) as temp_9am,
    cast(`9am_dew` as double) as dew_9am,
    cast(`9am_wind` as double) as wind_9am,

    cast(`12pm_temp` as double) as temp_12pm,
    cast(`12pm_dew` as double) as dew_12pm,
    cast(`12pm_wind` as double) as wind_12pm

from {{ source('marathon', 'bronze_weather') }}