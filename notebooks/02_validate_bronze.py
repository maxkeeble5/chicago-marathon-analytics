# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Data Validation
# MAGIC
# MAGIC This notebook validates the Bronze Delta tables created during ingestion before they are used by downstream dbt transformations.
# MAGIC
# MAGIC The checks verify table availability, required schema, critical fields, duplicate records, and expected marathon-year coverage.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Define the Bronze tables used throughout the validation checks.

# COMMAND ----------

from pyspark.sql import functions as F

tables = {
    "results": "marathon.analytics.bronze_results",
    "weather": "marathon.analytics.bronze_weather"
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Bronze Tables
# MAGIC
# MAGIC Confirm that each expected Bronze table exists in Unity Catalog and contains data.

# COMMAND ----------

for table_name in tables.values():
    assert spark.catalog.tableExists(table_name), \
        f"Missing Bronze table: {table_name}"

    row_count = spark.table(table_name).count()

    assert row_count > 0, \
        f"{table_name} is empty"

    print(f"PASS: {table_name} contains {row_count:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Required Schema
# MAGIC
# MAGIC Verify that both Bronze tables contain the columns required by downstream transformations and analysis.

# COMMAND ----------

results_df = spark.table(tables["results"])
weather_df = spark.table(tables["weather"])

required_results_columns = {
    "year",
    "place_overall",
    "athlete_name",
    "gender",
    "age_group",
    "country_ioc",
    "finish_time",
    "finish_time_seconds"
}

required_weather_columns = {
    "year",
    "race_date",
    "temp_6am",
    "dew_6am",
    "wind_6am",
    "temp_9am",
    "dew_9am",
    "wind_9am",
    "temp_12pm",
    "dew_12pm",
    "wind_12pm"
}

missing_results_columns = required_results_columns - set(results_df.columns)
missing_weather_columns = required_weather_columns - set(weather_df.columns)

assert not missing_results_columns, \
    f"bronze_results is missing columns: {missing_results_columns}"

assert not missing_weather_columns, \
    f"bronze_weather is missing columns: {missing_weather_columns}"

print("PASS: bronze_results contains all required columns")
print("PASS: bronze_weather contains all required columns")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Critical Fields
# MAGIC
# MAGIC Confirm that fields required for core downstream analysis contain no null values.

# COMMAND ----------

critical_columns = [
    "year",
    "gender",
    "finish_time_seconds"
]

for column in critical_columns:
    null_count = (
        results_df
        .filter(F.col(column).isNull())
        .count()
    )

    assert null_count == 0, \
        f"{column} contains {null_count:,} null values"

print("PASS: critical results fields contain no nulls")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Duplicate Records
# MAGIC
# MAGIC Confirm that the marathon results table does not contain exact duplicate records.

# COMMAND ----------

total_rows = results_df.count()
unique_rows = results_df.dropDuplicates().count()
duplicate_rows = total_rows - unique_rows

assert duplicate_rows == 0, \
    f"Found {duplicate_rows:,} duplicate rows"

print("PASS: no duplicate result records found")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Business Rules
# MAGIC
# MAGIC Confirm that results and weather data cover the expected 2000–2025 Chicago Marathon years, excluding the canceled 2020 race, and contain expected categorical values.

# COMMAND ----------

expected_years = set(range(2000, 2026)) - {2020}

results_years = {
    row["year"]
    for row in results_df.select("year").distinct().collect()
}

weather_years = {
    row["year"]
    for row in weather_df.select("year").distinct().collect()
}

assert results_years == expected_years, \
    f"Unexpected results years: {sorted(results_years)}"

assert weather_years == expected_years, \
    f"Unexpected weather years: {sorted(weather_years)}"


valid_genders = {
    "Man",
    "Woman",
    "Non-binary",
    "Not Specified"
}

gender_values = {
    row["gender"]
    for row in results_df.select("gender").distinct().collect()
}

assert gender_values.issubset(valid_genders), \
    f"Unexpected gender values found: {gender_values}"


assert weather_df.count() == 25, \
    f"Expected 25 weather rows, found {weather_df.count()}"

assert weather_df.select("year").distinct().count() == 25, \
    "Weather contains duplicate race years"


print("PASS: expected race years are present")
print("PASS: gender values are valid")
print("PASS: weather contains one row per race year")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Summary
# MAGIC
# MAGIC Confirm that all Bronze validation checks passed successfully.

# COMMAND ----------

print("BRONZE VALIDATION PASSED")