# Databricks notebook source
# MAGIC %md
# MAGIC # Raw Data Ingestion and Bronze Tables
# MAGIC
# MAGIC This notebook ingests Chicago Marathon results and historical race-day weather data, stores the raw source files in a Unity Catalog Volume, and creates Bronze Delta tables for downstream dbt transformations.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Import required libraries and define the Unity Catalog Volume used for raw data storage.

# COMMAND ----------

# MAGIC %pip install kagglehub

# COMMAND ----------

import kagglehub
import os
import shutil
import requests
import pandas as pd

from pyspark.sql import functions as F

volume_path = "/Volumes/marathon/analytics/raw_files"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chicago Marathon Results
# MAGIC
# MAGIC Download the 2000–2025 Chicago Marathon finisher dataset from Kaggle and copy the source file to the Unity Catalog Volume.

# COMMAND ----------

results_download_path = kagglehub.dataset_download(
    "ramostherunning/chicago-marathon-2000-2025"
)

results_filename = "Chicago_Marathon_2000-2025.csv"
results_source_path = os.path.join(
    results_download_path,
    results_filename
)
results_raw_path = os.path.join(
    volume_path,
    results_filename
)

shutil.copy(
    results_source_path,
    results_raw_path
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Historical Race-Day Weather
# MAGIC
# MAGIC Use the historical weather file to identify Chicago Marathon race dates through 2023, add the 2024 and 2025 race dates, and retrieve consistent hourly weather observations from Open-Meteo for each race.

# COMMAND ----------

historical_weather_path = os.path.join(
    volume_path,
    "Chicago Marathon Weather.csv"
)

historical_weather_dates = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(historical_weather_path)
)

race_dates = (
    historical_weather_dates
    .select(
        F.col("Year").cast("int").alias("year"),
        F.to_date("Date", "MMM d, yyyy").alias("race_date")
    )
    .filter(
        (F.col("year") >= 2000) &
        (F.col("year") <= 2023) &
        (F.col("year") != 2020)
    )
    .orderBy("year")
    .collect()
)

race_dates = [
    (row["year"], row["race_date"].strftime("%Y-%m-%d"))
    for row in race_dates
]

race_dates.extend([
    (2024, "2024-10-13"),
    (2025, "2025-10-12")
])

race_dates = sorted(race_dates)

# COMMAND ----------

weather_records = []

for year, race_date in race_dates:
    params = {
        "latitude": 41.8781,
        "longitude": -87.6298,
        "start_date": race_date,
        "end_date": race_date,
        "hourly": "temperature_2m,dew_point_2m,wind_speed_10m",
        "temperature_unit": "fahrenheit",
        "wind_speed_unit": "mph",
        "timezone": "America/Chicago"
    }

    response = requests.get(
        "https://archive-api.open-meteo.com/v1/archive",
        params=params
    )
    response.raise_for_status()

    hourly = response.json()["hourly"]

    hour_index = {
        timestamp[-5:]: i
        for i, timestamp in enumerate(hourly["time"])
    }

    weather_records.append({
        "year": year,
        "race_date": race_date,
        "temp_6am": hourly["temperature_2m"][hour_index["06:00"]],
        "dew_6am": hourly["dew_point_2m"][hour_index["06:00"]],
        "wind_6am": hourly["wind_speed_10m"][hour_index["06:00"]],
        "temp_9am": hourly["temperature_2m"][hour_index["09:00"]],
        "dew_9am": hourly["dew_point_2m"][hour_index["09:00"]],
        "wind_9am": hourly["wind_speed_10m"][hour_index["09:00"]],
        "temp_12pm": hourly["temperature_2m"][hour_index["12:00"]],
        "dew_12pm": hourly["dew_point_2m"][hour_index["12:00"]],
        "wind_12pm": hourly["wind_speed_10m"][hour_index["12:00"]]
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Raw Weather Data
# MAGIC
# MAGIC Convert the Open-Meteo responses into a structured dataset and save the 2000–2025 race-day weather data to the Unity Catalog Volume.

# COMMAND ----------

weather_raw_path = os.path.join(
    volume_path,
    "Chicago_Marathon_Weather_2000-2025.csv"
)

weather_pd = pd.DataFrame(weather_records)
weather_pd.to_csv(weather_raw_path, index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Bronze Tables
# MAGIC
# MAGIC Load the raw marathon results and weather files and write them as Delta tables in Unity Catalog for downstream transformation with dbt.

# COMMAND ----------

def write_bronze_table(df, table_name):
    (
        df.write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .format("delta")
        .saveAsTable(table_name)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Marathon Results
# MAGIC
# MAGIC Load the raw finisher dataset, select the fields used by the project, and write the results to the Bronze layer.

# COMMAND ----------

results_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .option("sep", ";")
    .csv(results_raw_path)
    .withColumn("finish_time", F.col("finish_time").cast("string"))
    .select(
        "year",
        "place_overall",
        "place_gender",
        "bib",
        "athlete_name",
        "athlete_id",
        "gender",
        "age_group",
        "country_ioc",
        "is_usa",
        "is_brazil",
        "finish_time",
        "finish_time_seconds",
        "pace_seconds_per_km",
        "half_split_seconds",
        "decade",
        "is_post_major"
    )
)

write_bronze_table(
    results_df,
    "marathon.analytics.bronze_results"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Race-Day Weather
# MAGIC
# MAGIC Load the Open-Meteo race-day weather dataset and write it to the Bronze layer.

# COMMAND ----------

weather_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(weather_raw_path)
)

write_bronze_table(
    weather_df,
    "marathon.analytics.bronze_weather"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion Summary
# MAGIC
# MAGIC Confirm that both Bronze tables were created successfully and report their final row counts.

# COMMAND ----------

print(f"bronze_results: {results_df.count():,} rows")
print(f"bronze_weather: {weather_df.count():,} rows")