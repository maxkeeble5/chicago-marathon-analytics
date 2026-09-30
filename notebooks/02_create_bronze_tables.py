# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Delta Tables
# MAGIC
# MAGIC This notebook reads the raw marathon datasets from the Unity Catalog Volume and creates Bronze Delta tables using standardized column names.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def standardize_column_names(df):
    """
    Standardize column names for Delta tables.
    """
    for old_name in df.columns:
        new_name = (
            old_name.strip()
                    .lower()
                    .replace(" ", "_")
                    .replace("(", "")
                    .replace(")", "")
                    .replace("/", "_")
                    .replace("%", "pct")
        )

        if old_name != new_name:
            df = df.withColumnRenamed(old_name, new_name)

    return df

# COMMAND ----------

def write_bronze_table(df, table_name):
    (
        df.write
            .mode("overwrite")
            .format("delta")
            .saveAsTable(table_name)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

volume_path = "/Volumes/marathon/analytics/raw_files"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Results

# COMMAND ----------

results_df = (
    spark.read
         .option("header", True)
         .csv(f"{volume_path}/Chicago Marathon Results.csv")
)

results_df = standardize_column_names(results_df)

write_bronze_table(
    results_df,
    "marathon.analytics.bronze_results"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Weather

# COMMAND ----------

weather_df = (
    spark.read
         .option("header", True)
         .csv(f"{volume_path}/Chicago Marathon Weather.csv")
)

weather_df = standardize_column_names(weather_df)

write_bronze_table(
    weather_df,
    "marathon.analytics.bronze_weather"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Qualifying Times

# COMMAND ----------

qualifying_df = (
    spark.read
         .option("header", True)
         .csv(f"{volume_path}/Chicago Marathon Qualifying Times.csv")
)

qualifying_df = standardize_column_names(qualifying_df)

write_bronze_table(
    qualifying_df,
    "marathon.analytics.bronze_qualifying_times"
)