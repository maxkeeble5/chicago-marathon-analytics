# Chicago Marathon Analytics

An end-to-end analytics engineering project analyzing more than 930,000 Chicago Marathon finishers from 2000–2025.

The project was built as a hands-on exploration of Databricks and dbt, using Chicago Marathon results to practice building a complete analytics workflow from raw data ingestion and validation through transformation, testing, and analysis.

## Project Overview

The project analyzes how the Chicago Marathon has evolved over the past 25 years, including:

- Long-term participation trends
- Changes in runner demographics
- Gender and age composition
- Growth in international participation
- Finish-time trends and changes in the distribution of finishers
- The relationship between race-day temperature and finishing times
- Potential 2026 performance under different race-day temperature scenarios

The dataset itself does not require a distributed data platform at this scale. Databricks and dbt were intentionally used to explore how the same data could be structured using modern analytics engineering practices.

## Tech Stack

- Databricks
- Unity Catalog
- Delta Lake
- PySpark
- Python
- dbt
- SQL
- pandas
- Matplotlib
- statsmodels

## Architecture

```text
Kaggle Marathon Results          Open-Meteo Weather API
          │                              │
          └──────────────┬───────────────┘
                         ↓
               Python / PySpark Ingestion
                         ↓
                 Unity Catalog Volume
                    (raw files)
                         ↓
                  Bronze Delta Tables
                         ↓
                  dbt Staging Views
                         ↓
                Runner Analytics Mart
                         ↓
              Databricks Analysis Notebook
```

The pipeline intentionally keeps the transformation layer compact. Bronze tables preserve the ingested source data, dbt staging models standardize the fields used for analysis, and the final runner-level mart joins marathon results with race-day weather while preserving individual finisher grain.

## Data Sources

### Chicago Marathon Results

Marathon results are sourced from the public [Chicago Marathon 2000–2025](https://www.kaggle.com/datasets/ramostherunning/chicago-marathon-2000-2025) dataset published by Victor Ramos on Kaggle.

The dataset contains 931,958 finisher records across 25 Chicago Marathons. The 2020 race is absent because the marathon was canceled.

The source data includes runner placement, gender, age group, country, finish time, and half-marathon split information.

The repository does not redistribute the Kaggle dataset. Source data is downloaded during ingestion using KaggleHub.

### Weather

Historical race-day weather is retrieved from the Open-Meteo Historical Weather API for each Chicago Marathon race date.

The weather dataset contains temperature, dew point, and wind speed observations at 6 AM, 9 AM, and 12 PM for each race year.

## Data Pipeline

### 1. Ingestion

`notebooks/01_ingest_bronze.py`

The ingestion notebook:

- Downloads Chicago Marathon results using KaggleHub
- Retrieves historical race-day weather from Open-Meteo
- Stores raw source files in a Unity Catalog Volume
- Creates Delta Lake Bronze tables for results and weather

### 2. Bronze Validation

`notebooks/02_validate_bronze.py`

The validation notebook checks:

- Required schemas
- Critical null fields
- Duplicate records
- Expected race years
- Core business rules

The validation layer provides a data-quality checkpoint before dbt transformations run.

### 3. dbt Transformations

dbt transforms the Bronze tables into analysis-ready models.

```text
bronze_results ──→ stg_results ──┐
                                 ├──→ mart_runner_results
bronze_weather ──→ stg_weather ──┘
```

The dbt project contains:

- `stg_results` — standardized marathon finisher data
- `stg_weather` — standardized race-day weather data
- `mart_runner_results` — runner-level analytics table combining finisher and weather data

dbt tests validate critical fields, accepted gender values, race-year uniqueness in the weather data, and the presence of weather data in the final mart.

## Analysis

`notebooks/03_analysis.py`

The final Databricks analysis uses the dbt runner-level mart and focuses on participation, demographics, finish-time trends, weather, and a 2026 performance scenario.

### Participation and Demographics

The analysis examines long-term participation growth along with changes in gender, age, and international representation.

Among the major trends:

- Marathon participation reached record levels by 2025
- Women increased from approximately 40% of finishers in 2000 to roughly 45–47% in recent years
- The finisher population has gradually shifted toward older age groups
- International representation increased substantially over the period

### Finish-Time Trends

Median finish time is used to measure the typical runner while reducing the influence of unusually slow finishing times.

The analysis also divides finishers into broad finish-time ranges to distinguish changes in overall performance from changes in the composition of the field.

Recent races show a substantial increase in the number of runners finishing under 3:30, while participation has also grown across the broader field.

### Weather and Performance

Race-level median finishing times are compared with 9 AM race-day temperature.

Across the full historical dataset:

- Correlation between 9 AM temperature and median finish time: **0.71**
- Simple linear-model R²: **0.50**
- Estimated association: approximately **0.82 additional minutes of median finish time per 1°F increase**

The results indicate a strong historical association between warmer race conditions and slower finishing times, while also showing that weather does not account for all year-to-year variation.

### 2026 Performance Scenario

The project concludes with a simple scenario analysis rather than a precise forecast.

The recent 2023–2025 races provide a baseline for the modern marathon field. A historical temperature model is then used to estimate how different 2026 race-day conditions could shift the median finish time.

The scenario model excludes:

- **2007**, when extreme heat caused the race to be stopped early
- **2021–2022**, when pandemic-related disruptions made the race fields less comparable with typical years

Estimated 2026 median finish times are:

| Scenario | 9 AM Temperature | Estimated Median Finish |
|---|---:|---:|
| Cool | 42°F | 4:04:06 |
| Moderate | 50°F | 4:09:22 |
| Warm | 60°F | 4:15:57 |

These estimates are intended as weather scenarios rather than precise predictions. Runner composition and other race-year factors contribute substantially to finishing times.

## Repository Structure

```text
chicago-marathon-analytics/
│
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml
│   │   │   ├── schema.yml
│   │   │   ├── stg_results.sql
│   │   │   └── stg_weather.sql
│   │   └── marts/
│   │       ├── schema.yml
│   │       └── mart_runner_results.sql
│   └── dbt_project.yml
│
├── notebooks/
│   ├── 01_ingest_bronze.py
│   ├── 02_validate_bronze.py
│   └── 03_analysis.py
│
├── .gitignore
├── LICENSE
└── README.md
```

## Running the Project

The project requires a Databricks workspace with Unity Catalog and a configured dbt Databricks connection.

The pipeline runs in the following order:

```text
01_ingest_bronze
        ↓
02_validate_bronze
        ↓
      dbt build
        ↓
03_analysis
```

The dbt project can be built from the `dbt` directory:

```bash
dbt build
```

The completed dbt build creates the staging views and runner analytics mart and executes the associated data-quality tests.

## License

MIT