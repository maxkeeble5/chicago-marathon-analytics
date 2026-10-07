# Databricks notebook source
# MAGIC %md
# MAGIC # Chicago Marathon Analysis
# MAGIC
# MAGIC This notebook analyzes Chicago Marathon participation, runner demographics, finish-time trends, and race-day weather from 2000–2025 using the curated runner-level dataset produced by the dbt pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Load the runner-level analytics mart produced by the dbt pipeline.

# COMMAND ----------

from pyspark.sql import functions as F

runner_df = spark.table(
    "marathon.dbt_dev.mart_runner_results"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dataset Overview
# MAGIC
# MAGIC Confirm the scope and structure of the curated dataset before beginning the analysis.

# COMMAND ----------

runner_df.select(
    F.count("*").alias("runner_records"),
    F.countDistinct("year").alias("race_years"),
    F.min("year").alias("first_year"),
    F.max("year").alias("last_year")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Participation Trends
# MAGIC
# MAGIC Examine how Chicago Marathon participation has changed since 2000, including long-term growth and the race's recovery following the canceled 2020 marathon.

# COMMAND ----------

participation_df = (
    runner_df
    .groupBy("year")
    .agg(
        F.count("*").alias("finishers")
    )
    .orderBy("year")
)

import matplotlib.pyplot as plt

participation_pd = participation_df.toPandas()

plt.figure(figsize=(12, 6))

plt.plot(
    participation_pd["year"],
    participation_pd["finishers"],
    marker="o"
)

plt.title("Chicago Marathon Finishers by Year")
plt.xlabel("Year")
plt.ylabel("Finishers")
plt.grid(alpha=0.3)

plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC > **Observation:** Chicago Marathon participation generally increased from 2000 through 2019, although several years show notable declines. The sharp drop in 2007 reflects unusually severe race-day heat, which led organizers to halt the race and prevented thousands of starters from recording an official finish. Participation also fell substantially in 2021 following the cancellation of the 2020 race (Covid), before rebounding to record levels by 2025.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Runner Demographics
# MAGIC
# MAGIC Examine how the composition of Chicago Marathon finishers has changed over time, focusing on gender, age group, and international participation.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gender Composition

# COMMAND ----------

gender_df = (
    runner_df
    .groupBy("year", "gender")
    .agg(
        F.count("*").alias("finishers")
    )
)

year_totals = (
    runner_df
    .groupBy("year")
    .agg(
        F.count("*").alias("year_finishers")
    )
)

gender_share_df = (
    gender_df
    .join(year_totals, on="year")
    .withColumn(
        "share_pct",
        F.round(
            (F.col("finishers") / F.col("year_finishers")) * 100,
            1
        )
    )
    .orderBy("year", "gender")
)

gender_plot_pd = (
    gender_share_df
    .filter(F.col("gender") != "Not Specified")
    .select("year", "gender", "share_pct")
    .toPandas()
)

gender_pivot = gender_plot_pd.pivot(
    index="year",
    columns="gender",
    values="share_pct"
)

plt.figure(figsize=(12, 6))

for gender in gender_pivot.columns:
    plt.plot(
        gender_pivot.index,
        gender_pivot[gender],
        marker="o",
        label=gender
    )

plt.title("Chicago Marathon Finisher Gender Composition")
plt.xlabel("Year")
plt.ylabel("Share of Finishers (%)")
plt.legend(title="Gender")
plt.grid(alpha=0.3)

plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC > **Observation:** Women increased from about 40% of finishers in 2000 to roughly 45–47% in most recent years, narrowing the gender gap substantially over time. The field was closest to an even split in 2017, when women represented 48.5% of finishers. The 2007 race stands out as a temporary reversal, with the men's share rising to about 60% during the extreme-heat race.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Age Group Composition
# MAGIC
# MAGIC Group finishers into broader age ranges and compare their share of the marathon field over time.

# COMMAND ----------

age_trend_df = (
    runner_df
    .withColumn(
        "age_range",
        F.when(
            F.col("age_group").isin("19 and under", "20-24"),
            "Under 25"
        )
        .when(
            F.col("age_group").isin("25-29", "30-34"),
            "25-34"
        )
        .when(
            F.col("age_group").isin("35-39", "40-44"),
            "35-44"
        )
        .when(
            F.col("age_group").isin("45-49", "50-54"),
            "45-54"
        )
        .when(
            F.col("age_group").isin("55-59", "60-64"),
            "55-64"
        )
        .when(
            F.col("age_group").isin("65-69", "70-74", "75-79", "80+"),
            "65+"
        )
    )
    .filter(F.col("age_range").isNotNull())
    .groupBy("year", "age_range")
    .agg(F.count("*").alias("finishers"))
)

age_totals = (
    age_trend_df
    .groupBy("year")
    .agg(F.sum("finishers").alias("year_finishers"))
)

age_plot_pd = (
    age_trend_df
    .join(age_totals, on="year")
    .withColumn(
        "share_pct",
        F.round(
            F.col("finishers") / F.col("year_finishers") * 100,
            1
        )
    )
    .select("year", "age_range", "share_pct")
    .toPandas()
)

age_pivot = age_plot_pd.pivot(
    index="year",
    columns="age_range",
    values="share_pct"
)

age_order = [
    "Under 25",
    "25-34",
    "35-44",
    "45-54",
    "55-64",
    "65+"
]

plt.figure(figsize=(12, 6))

for age_range in age_order:
    plt.plot(
        age_pivot.index,
        age_pivot[age_range],
        marker="o",
        label=age_range
    )

plt.title("Chicago Marathon Finisher Age Composition")
plt.xlabel("Year")
plt.ylabel("Share of Finishers (%)")
plt.legend(title="Age Range")
plt.grid(alpha=0.3)

plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC > **Observation:** The Chicago Marathon field has gradually shifted toward older runners. Finishers ages 25–34 declined from about 39% of the field in 2000 to 34% in 2025, while the share ages 45–54 increased from about 15% to 21%. Runners ages 55–64 and 65+ also became substantially more common, while runners under 25 declined as a share of finishers. Ages 35–44 remained comparatively stable across the period.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Age Composition by Gender
# MAGIC
# MAGIC Compare the age distribution of men and women to determine whether the overall age patterns differ meaningfully by gender.

# COMMAND ----------

age_gender_df = (
    runner_df
    .filter(F.col("gender").isin("Man", "Woman"))
    .withColumn(
        "age_range",
        F.when(
            F.col("age_group").isin("19 and under", "20-24"),
            "Under 25"
        )
        .when(
            F.col("age_group").isin("25-29", "30-34"),
            "25-34"
        )
        .when(
            F.col("age_group").isin("35-39", "40-44"),
            "35-44"
        )
        .when(
            F.col("age_group").isin("45-49", "50-54"),
            "45-54"
        )
        .when(
            F.col("age_group").isin("55-59", "60-64"),
            "55-64"
        )
        .when(
            F.col("age_group").isin("65-69", "70-74", "75-79", "80+"),
            "65+"
        )
    )
    .filter(F.col("age_range").isNotNull())
    .groupBy("gender", "age_range")
    .agg(F.count("*").alias("finishers"))
)

gender_totals = (
    age_gender_df
    .groupBy("gender")
    .agg(F.sum("finishers").alias("gender_finishers"))
)

age_gender_pd = (
    age_gender_df
    .join(gender_totals, on="gender")
    .withColumn(
        "share_pct",
        F.col("finishers") / F.col("gender_finishers") * 100
    )
    .select("gender", "age_range", "share_pct")
    .toPandas()
)

age_gender_pivot = (
    age_gender_pd
    .pivot(
        index="age_range",
        columns="gender",
        values="share_pct"
    )
    .reindex([
        "Under 25",
        "25-34",
        "35-44",
        "45-54",
        "55-64",
        "65+"
    ])
)

age_gender_pivot.plot(
    kind="bar",
    figsize=(12, 6)
)

plt.title("Chicago Marathon Age Composition by Gender")
plt.xlabel("Age Range")
plt.ylabel("Share of Gender (%)")
plt.xticks(rotation=0)
plt.legend(title="Gender")
plt.grid(axis="y", alpha=0.3)

plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC > **Observation:** Female finishers skew younger than male finishers. Women are substantially more concentrated in the Under 25 and 25–34 age ranges, while men make up a larger share of the 35–44 and older age ranges. The difference becomes increasingly pronounced among older finishers, suggesting that the gender gap in marathon participation varies considerably by age.

# COMMAND ----------

# MAGIC %md
# MAGIC ### International Participation
# MAGIC
# MAGIC Examine how the share of Chicago Marathon finishers from outside the United States has changed over time.

# COMMAND ----------

international_df = (
    runner_df
    .filter(F.col("is_usa").isNotNull())
    .groupBy("year")
    .agg(
        F.count("*").alias("finishers"),
        F.sum(
            F.when(F.col("is_usa") == False, 1).otherwise(0)
        ).alias("international_finishers")
    )
    .withColumn(
        "international_share_pct",
        F.col("international_finishers") / F.col("finishers") * 100
    )
    .orderBy("year")
)

international_pd = international_df.toPandas()

plt.figure(figsize=(12, 6))

plt.plot(
    international_pd["year"],
    international_pd["international_share_pct"],
    marker="o"
)

plt.title("International Share of Chicago Marathon Finishers")
plt.xlabel("Year")
plt.ylabel("International Finishers (%)")
plt.grid(alpha=0.3)

plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC > **Observation:** International participation has grown substantially over the history of the dataset. Finishers representing countries outside the United States increased from about 9% of the field in 2000 to 43% in 2025. After a sharp decline in 2021, international representation rebounded quickly and reached its highest levels in 2024–2025.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Finish-Time Trends
# MAGIC
# MAGIC Examine how the typical Chicago Marathon finishing time has changed since 2000. Median finish time is used to reduce the influence of unusually slow finishing times.

# COMMAND ----------

import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

finish_time_pd = finish_time_df.toPandas()

def format_finish_time(minutes, pos):
    hours = int(minutes // 60)
    mins = int(minutes % 60)
    return f"{hours}:{mins:02d}"

plt.figure(figsize=(12, 6))

plt.plot(
    finish_time_pd["year"],
    finish_time_pd["median_finish_minutes"],
    marker="o"
)

plt.title("Chicago Marathon Median Finish Time by Year")
plt.xlabel("Year")
plt.ylabel("Median Finish Time")
plt.gca().yaxis.set_major_formatter(
    FuncFormatter(format_finish_time)
)
plt.grid(alpha=0.3)

plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC > **Observation:** Median finish times vary substantially across race years, with 2007 standing out as the slowest year in the dataset. Finish times were also elevated in several other years, while the fastest median times occur in the most recent races from 2022–2025. The year-to-year variation suggests that race conditions may play an important role in marathon performance.
# MAGIC
# MAGIC > Additional Context: The recent improvement in finish times coincides with a broader post-pandemic running boom. Strava reported a 90% increase in outdoor running activities from 2019 to 2020, and by 2022 the share of Strava runners completing a marathon had nearly doubled from 2021. This provides useful context for the shift seen in Chicago, although the available data cannot establish that increased running participation caused faster marathon times.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Finish-Time Distribution
# MAGIC
# MAGIC Examine how the number of finishers within different finishing-time ranges has changed as the Chicago Marathon field has grown.

# COMMAND ----------

plt.figure(figsize=(14, 6))

for bucket in bucket_order:
    plt.plot(
        finish_bucket_pivot.index,
        finish_bucket_pivot[bucket],
        marker="o",
        label=bucket
    )

plt.title("Chicago Marathon Finishers by Finish-Time Range")
plt.xlabel("Year")
plt.ylabel("Finishers")
plt.legend(title="Finish Time")
plt.grid(alpha=0.3)

plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC > **Observation:** The recent decline in median finish time coincides with a substantial shift in the composition of the field. While all three finish-time groups have grown over the long term, the number of runners finishing under 3:30 increased particularly sharply after 2022, reaching more than double its typical level during much of the 2000s and 2010s. This suggests that the faster median times observed in recent races are driven at least partly by a larger concentration of faster finishers rather than a simple long-term improvement in finishing times.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Weather and Performance
# MAGIC
# MAGIC Examine whether race-day weather conditions are associated with differences in Chicago Marathon finishing times. Because weather is measured at the race level, runner results are aggregated to the median finish time for each race year.

# COMMAND ----------

weather_performance_df = (
    runner_df
    .groupBy("year")
    .agg(
        F.expr(
            "percentile_approx(finish_time_seconds, 0.5)"
        ).alias("median_finish_seconds"),
        F.first("temp_9am").alias("temp_9am")
    )
    .withColumn(
        "median_finish_minutes",
        F.col("median_finish_seconds") / 60
    )
    .orderBy("year")
)

weather_performance_pd = weather_performance_df.toPandas()

plt.figure(figsize=(10, 6))

plt.scatter(
    weather_performance_pd["temp_9am"],
    weather_performance_pd["median_finish_minutes"]
)

for _, row in weather_performance_pd.iterrows():
    plt.annotate(
        int(row["year"]),
        (row["temp_9am"], row["median_finish_minutes"]),
        xytext=(4, 4),
        textcoords="offset points",
        fontsize=8
    )

plt.title("9 AM Temperature vs. Median Finish Time")
plt.xlabel("9 AM Temperature (°F)")
plt.ylabel("Median Finish Time")

plt.gca().yaxis.set_major_formatter(
    FuncFormatter(format_finish_time)
)

plt.grid(alpha=0.3)

plt.show()

# COMMAND ----------

from scipy.stats import pearsonr, linregress

x = weather_performance_pd["temp_9am"]
y = weather_performance_pd["median_finish_minutes"]

correlation, p_value = pearsonr(x, y)
regression = linregress(x, y)

print(f"Correlation: {correlation:.3f}")
print(f"R²: {regression.rvalue**2:.3f}")
print(f"Temperature coefficient: {regression.slope:.2f} minutes per °F")
print(f"P-value: {regression.pvalue:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC > **Observation:** Warmer race-day temperatures are strongly associated with slower Chicago Marathon finish times. Across race years, 9 AM temperature has a correlation of 0.71 with median finish time and accounts for about 50% of the observed year-to-year variation in a simple linear model. Each additional 1°F at 9 AM is associated with approximately 0.82 minutes (49 seconds) of additional median finish time.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2026 Performance Scenario
# MAGIC
# MAGIC Estimate how different race-day temperatures could affect the median finish time for the 2026 Chicago Marathon.
# MAGIC
# MAGIC The scenario uses the average median finish time and 9 AM temperature from 2023–2025 as a baseline for the modern race field. The historical relationship between 9 AM temperature and median finish time is then used to estimate outcomes under cool, moderate, and warm race-day conditions.
# MAGIC
# MAGIC The historical temperature model excludes 2007 because extreme heat caused the race to be stopped early, and excludes 2021–2022 because pandemic-related disruptions made those fields less comparable with typical race years.

# COMMAND ----------

import statsmodels.api as sm

forecast_model_df = (
    runner_df
    .filter(~F.col("year").isin([2007, 2021, 2022]))
    .groupBy("year")
    .agg(
        F.expr(
            "percentile_approx(finish_time_seconds, 0.5)"
        ).alias("median_finish_seconds"),
        F.first("temp_9am").alias("temp_9am")
    )
    .withColumn(
        "median_finish_minutes",
        F.col("median_finish_seconds") / 60
    )
    .orderBy("year")
)

forecast_model_pd = forecast_model_df.toPandas()

X = forecast_model_pd[["temp_9am"]]
X = sm.add_constant(X)

y = forecast_model_pd["median_finish_minutes"]

forecast_model = sm.OLS(y, X).fit()

print(forecast_model.summary())

# COMMAND ----------

modern_baseline_minutes = 251.67
modern_baseline_temp = 53.5
temp_coefficient = forecast_model.params["temp_9am"]

scenarios = {
    "Cool (≤45°F)": 42,
    "Moderate (46–55°F)": 50,
    "Warm (56–65°F)": 60
}

scenario_results = []

for scenario, temp in scenarios.items():
    predicted_minutes = (
        modern_baseline_minutes
        + temp_coefficient * (temp - modern_baseline_temp)
    )

    hours = int(predicted_minutes // 60)
    minutes = int(predicted_minutes % 60)
    seconds = int(round((predicted_minutes % 1) * 60))

    scenario_results.append({
        "scenario": scenario,
        "temp_9am": temp,
        "predicted_minutes": predicted_minutes,
        "predicted_finish_time": f"{hours}:{minutes:02d}:{seconds:02d}"
    })

scenario_pd = pd.DataFrame(scenario_results)

display(
    scenario_pd[
        [
            "scenario",
            "temp_9am",
            "predicted_finish_time"
        ]
    ]
)

plt.figure(figsize=(10, 6))

plt.bar(
    scenario_pd["scenario"],
    scenario_pd["predicted_minutes"]
)

plt.title("2026 Chicago Marathon Finish-Time Scenarios")
plt.xlabel("Race-Day Conditions")
plt.ylabel("Estimated Median Finish Time")

plt.gca().yaxis.set_major_formatter(
    FuncFormatter(format_finish_time)
)

plt.ylim(235, 265)
plt.grid(axis="y", alpha=0.3)

plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC > **Observation:** Based on the recent 2023–2025 field and the historical relationship between temperature and finish times, a cool 2026 race would correspond to an estimated median finish time of about 4:04, compared with 4:09 under moderate conditions and 4:16 under warm conditions. These estimates should be interpreted as weather scenarios rather than precise forecasts, since field composition and other race-year factors also contribute substantially to finishing times.