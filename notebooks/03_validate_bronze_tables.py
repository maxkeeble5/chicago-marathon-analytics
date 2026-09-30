# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Data Validation
# MAGIC
# MAGIC This notebook validates the Bronze Delta tables created during ingestion.
# MAGIC
# MAGIC The checks ensure that:
# MAGIC - Bronze tables exist
# MAGIC - Data has been loaded successfully
# MAGIC - Required columns are present
# MAGIC - Row counts are reasonable
# MAGIC - Data quality rules pass before downstream transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Import validation dependencies, define the Bronze tables to check, and record the validation start time.

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

tables = {
    "results": "marathon.analytics.bronze_results",
    "weather": "marathon.analytics.bronze_weather",
    "qualifying_times": "marathon.analytics.bronze_qualifying_times"
}

print(f"Validation started: {datetime.now()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Table Availability
# MAGIC
# MAGIC Confirm that each expected Bronze table exists in Unity Catalog before running downstream validation checks.

# COMMAND ----------

for name, table_name in tables.items():
    assert spark.catalog.tableExists(table_name), \
        f"Missing Bronze table: {table_name}"

    print(f"PASS: {table_name} exists")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Row Counts
# MAGIC
# MAGIC Confirm that each Bronze table contains data after ingestion. Empty tables indicate that the ingestion pipeline did not load records successfully.

# COMMAND ----------

row_counts = {}

for name, table_name in tables.items():
    count = spark.table(table_name).count()
    row_counts[name] = count

    assert count > 0, f"{table_name} is empty"

    print(f"{table_name}: {count:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review Delta Table Metadata
# MAGIC
# MAGIC Inspect the latest Delta Lake version, last update timestamp, and most recent operation for each Bronze table to verify successful writes and support basic data lineage.

# COMMAND ----------

for name, table_name in tables.items():
    history = (
        spark.sql(f"DESCRIBE HISTORY {table_name}")
             .select("version", "timestamp", "operation")
             .orderBy(F.desc("version"))
             .first()
    )

    print(
        f"{table_name}\n"
        f"  Delta version: {history['version']}\n"
        f"  Latest write:  {history['timestamp']}\n"
        f"  Operation:     {history['operation']}\n"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Required Schema
# MAGIC
# MAGIC Verify that the marathon results table contains all expected columns required for downstream transformations and analytics.

# COMMAND ----------

results_df = spark.table(tables["results"])

required_results_columns = {
    "race",
    "year",
    "name",
    "gender",
    "age",
    "country",
    "overall",
    "finish_time",
    "finish"
}

missing_columns = required_results_columns - set(results_df.columns)

assert not missing_columns, \
    f"bronze_results is missing columns: {missing_columns}"

print("PASS: bronze_results contains all required columns")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Required Fields
# MAGIC
# MAGIC Confirm that critical fields used throughout the pipeline contain no null values.
# MAGIC
# MAGIC Runner names are evaluated separately because a small number of missing names are expected in the source data and do not impact downstream analysis.

# COMMAND ----------

critical_columns = [
    "year",
    "gender",
    "age",
    "finish_time",
    "finish"
]

for column in critical_columns:
    null_count = (
        results_df
        .filter(F.col(column).isNull())
        .count()
    )

    assert null_count == 0, \
        f"{column} contains {null_count:,} null values"

    print(f"PASS: {column} contains no nulls")

missing_names = (
    results_df
    .filter(F.col("name").isNull())
    .count()
)

print(f"INFO: missing runner names = {missing_names:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check for Duplicate Records
# MAGIC
# MAGIC Identify duplicate rows in the marathon results table. Duplicate records may indicate ingestion issues or source data quality problems that should be reviewed before downstream processing.

# COMMAND ----------

total_rows = results_df.count()
unique_rows = results_df.dropDuplicates().count()

duplicate_rows = total_rows - unique_rows

print(f"Duplicate rows: {duplicate_rows:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Business Rules
# MAGIC
# MAGIC Verify that key data values fall within expected ranges and categories, including marathon years, participant ages, and gender values.

# COMMAND ----------

results_validation_df = (
    results_df
    .withColumn("year_int", F.col("year").cast("int"))
    .withColumn("age_int", F.col("age").cast("int"))
)

year_stats = (
    results_validation_df
    .agg(
        F.min("year_int").alias("min_year"),
        F.max("year_int").alias("max_year")
    )
    .first()
)

print(
    f"Year range: "
    f"{year_stats['min_year']}–{year_stats['max_year']}"
)

assert year_stats["min_year"] == 1996
assert year_stats["max_year"] == 2023

gender_values = {
    row["gender"]
    for row in results_df.select("gender").distinct().collect()
}

print("Gender values:", gender_values)

assert gender_values.issubset({"M", "F"}), \
    f"Unexpected gender values found: {gender_values}"

age_stats = (
    results_validation_df
    .agg(
        F.min("age_int").alias("min_age"),
        F.max("age_int").alias("max_age")
    )
    .first()
)

print(
    f"Age range: "
    f"{age_stats['min_age']}–{age_stats['max_age']}"
)

assert age_stats["min_age"] >= 18
assert age_stats["max_age"] <= 100

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Summary
# MAGIC
# MAGIC Summarize the overall validation results and confirm that all Bronze tables are ready for downstream transformations.

# COMMAND ----------

print("=" * 60)
print("BRONZE VALIDATION PASSED")
print(f"Results rows: {row_counts['results']:,}")
print(f"Weather rows: {row_counts['weather']:,}")
print(f"Qualifying rows: {row_counts['qualifying_times']:,}")
print("=" * 60)