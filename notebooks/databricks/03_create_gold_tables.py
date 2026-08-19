# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Spark SQL関数モジュールのインポート
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Load Silver and Master Species DataFrames from Tables
silver_df = spark.read.table("workspace.silver.gbif_occurrences")
species_master = spark.read.table("workspace.mstr.species_master")

# COMMAND ----------

# DBTITLE 1,species_join_with_master_data
silver_df_with_species_nm = silver_df.join(
    species_master,
    silver_df["query_scientific_name"] == species_master["scientific_name"],
    "left"
)

# COMMAND ----------

# DBTITLE 1,species_observations_by_scientific_name_summary
species_summary_df = (
    silver_df_with_species_nm
    .groupBy(
        "scientificName",
        "query_scientific_name",
        "japanese_name",
        "taxonomic_group",
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

# DBTITLE 1,Update Gold Table with Species Summary Records
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

# DBTITLE 1,country_code_observations_summary_by_scientific_name
country_summary_df = (
    silver_df_with_species_nm
    .groupBy(
        "countryCode",
        "query_scientific_name",
        "japanese_name",
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

# DBTITLE 1,Update Gold Table with Country Summary Records
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

# DBTITLE 1,yearly_observations_summary_by_species_scientific_name
year_summary_df = (
    silver_df_with_species_nm
    .filter(
        F.col("event_year").isNotNull()
    )
    .groupBy(
        "event_year",
        "query_scientific_name",
        "japanese_name",
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

# DBTITLE 1,Update Gold Table with Yearly Summary Records
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

# DBTITLE 1,basis_of_record_observations_by_species_summary
basis_summary_df = (
    silver_df_with_species_nm
    .groupBy(
        "basisOfRecord",
        "query_scientific_name",
        "japanese_name",
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

# DBTITLE 1,Overwrite Gold Table with Updated Observation Type Data
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

# DBTITLE 1,Filter and Select Valid Geographic Species Observations
from pyspark.sql import functions as F


geographic_observations_df = (
    silver_df_with_species_nm
    .filter(
        F.col("decimalLatitude").isNotNull()
    )
    .filter(
        F.col("decimalLongitude").isNotNull()
    )
    .select(
        "gbifID",
        "scientificName",
        "japanese_name",
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
        "japanese_name",
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