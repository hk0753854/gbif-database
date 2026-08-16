# Databricks notebook source
silver_df = spark.read.table("workspace.silver.gbif_occurrences")

# COMMAND ----------

# DBTITLE 1,species_summary
from pyspark.sql import functions as F


species_summary_df = (
    silver_df
    .groupBy(
        "scientificName",
        "query_scientific_name",
    )
    .agg(
        F.count("*").alias("observations")
    )
    .orderBy(
        F.desc("observations")
    )
)

display(species_summary_df)

# COMMAND ----------

gold_table = "workspace.gold.species_summary"

(
    species_summary_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(gold_table)
)

print(
    f"Gold table updated: {gold_table}, "
    f"records={species_summary_df.count()}"
)

# COMMAND ----------

# DBTITLE 1,Country別集計
from pyspark.sql import functions as F


country_summary_df = (
    silver_df
    .groupBy(
        "countryCode",
        "query_scientific_name",
    )
    .agg(
        F.count("*").alias("observations")
    )
    .orderBy(
        F.desc("observations")
    )
)

display(country_summary_df)

# COMMAND ----------

gold_table = "workspace.gold.country_summary"

(
    country_summary_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(gold_table)
)

print(
    f"Gold table updated: {gold_table}, "
    f"records={country_summary_df.count()}"
)

# COMMAND ----------

# DBTITLE 1,Year別集計
year_summary_df = (
    silver_df
    .filter(
        F.col("event_year").isNotNull()
    )
    .groupBy(
        "event_year",
        "query_scientific_name",
    )
    .agg(
        F.count("*").alias("observations")
    )
    .orderBy(
        "event_year",
        "query_scientific_name",
    )
)

display(year_summary_df)

# COMMAND ----------

gold_table = "workspace.gold.year_summary"

(
    year_summary_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(gold_table)
)

print(
    f"Gold table updated: {gold_table}, "
    f"records={year_summary_df.count()}"
)

# COMMAND ----------

# DBTITLE 1,観察タイプ別分析
basis_summary_df = (
    silver_df
    .groupBy(
        "basisOfRecord",
        "query_scientific_name",
    )
    .agg(
        F.count("*").alias("observations")
    )
    .orderBy(
        "query_scientific_name",
        F.desc("observations"),
    )
)

display(basis_summary_df)

# COMMAND ----------

gold_table = "workspace.gold.observation_type_summary"

(
    basis_summary_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(gold_table)
)

print(
    f"Gold table updated: {gold_table}, "
    f"records={basis_summary_df.count()}"
)

# COMMAND ----------

# DBTITLE 1,地理情報のGold Table
from pyspark.sql import functions as F


geographic_observations_df = (
    silver_df
    .filter(
        F.col("decimalLatitude").isNotNull()
    )
    .filter(
        F.col("decimalLongitude").isNotNull()
    )
    .select(
        "gbifID",
        "scientificName",
        "query_scientific_name",
        "countryCode",
        "decimalLatitude",
        "decimalLongitude",
        "eventDate",
        "event_year",
        "event_month",
        "event_day",
    )
)

display(geographic_observations_df)

# COMMAND ----------

# 簡易的な分析用のため、緯度経度は0.1度程度にまとめる（近い観察地点をある程度まとめておく）
geographic_summary_df = (
    geographic_observations_df
    .withColumn(
        "latitude_grid",
        F.round(F.col("decimalLatitude"), 1)
    )
    .withColumn(
        "longitude_grid",
        F.round(F.col("decimalLongitude"), 1)
    )
    .groupBy(
        "latitude_grid",
        "longitude_grid",
        "query_scientific_name",
    )
    .agg(
        F.count("*").alias("observations")
    )
    .orderBy(
        "query_scientific_name",
        F.desc("observations"),
    )
)

display(geographic_summary_df)

# COMMAND ----------

gold_table = "workspace.gold.geographic_summary"

(
    geographic_summary_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(gold_table)
)

print(
    f"Gold table updated: {gold_table}, "
    f"records={geographic_summary_df.count()}"
)