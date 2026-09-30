# Chicago Marathon Analytics

An end-to-end analytics engineering project that uses Databricks, Delta Lake, dbt, SQL, and Python to build a modern data platform for analyzing Chicago Marathon results.

## Project Goals

This project answers questions such as:

- How did participation change after COVID?
- Which demographics have improved the most over time?
- How much does race-day weather impact finishing times?
- How has the marathon evolved from 1996–2023?

Rather than focusing only on analysis, this project demonstrates how to build a production-style analytics pipeline using modern data engineering tools.

## Tech Stack

- Databricks
- Unity Catalog
- Delta Lake
- PySpark
- Python
- dbt
- SQL
- Looker Studio
- Git
- GitHub

## Planned Architecture

Kaggle Dataset
      ↓
Python / KaggleHub Ingestion
      ↓
Unity Catalog Volume
(raw source files)
      ↓
Bronze Delta Tables
      ↓
dbt Transformations
      ↓
Silver Models
      ↓
Gold Analytics Marts
      ↓
Looker Studio

## Project Status

- ✅ Public GitHub repository created
- ✅ Databricks environment configured
- ✅ Kaggle ingestion connection established
- ✅ Source dataset profiled
- ✅ Unity Catalog Volume created
- ⬜ Raw files staged in Unity Catalog
- ⬜ Bronze Delta tables created
- ⬜ dbt project initialized
- ⬜ Silver models
- ⬜ Gold analytics marts
- ⬜ Looker Studio dashboard

## Data Source

This project uses the public Chicago Marathon Results dataset published by Brian Rock on Kaggle.

The underlying marathon results originate from the official Chicago Marathon results website, and the weather data originates from WeatherSpark.

This repository does not redistribute the source dataset. Data is downloaded during ingestion using KaggleHub.

## License

MIT
