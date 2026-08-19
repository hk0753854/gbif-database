# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///

# DBTITLE 1,Import Spark SQL Functions
from pyspark.sql import functions as F


# COMMAND ----------

# DBTITLE 1,Load Silver and Master Species DataFrames
silver_df = spark.read.table(
    "workspace.silver.gbif_occurrences"
)

species_master = spark.read.table(
    "workspace.mstr.species_master"
)


# COMMAND ----------

# DBTITLE 1,Join Occurrence Data with Species Master
silver_df_with_species_nm = (
    silver_df.join(
        species_master,
        silver_df["query_scientific_name"]
        == species_master["scientific_name"],
        "left",
    )
)


# COMMAND ----------

# DBTITLE 1,Create Species Summary
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

# DBTITLE 1,Write Species Summary to Gold
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

# DBTITLE 1,Create Country Summary
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

# DBTITLE 1,Write Country Summary to Gold
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

# DBTITLE 1,Create Yearly Observation Summary
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

# DBTITLE 1,Write Yearly Summary to Gold
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

# DBTITLE 1,Create Observation Type Summary
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

# DBTITLE 1,Write Observation Type Summary to Gold
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

# DBTITLE 1,Filter Valid Geographic Observations
geographic_observations_df = (
    silver_df_with_species_nm
    .filter(
        F.col("decimalLatitude").isNotNull()
    )
    .filter(
        F.col("decimalLongitude").isNotNull()
    )
    .filter(
        (F.col("decimalLatitude") >= -90)
        & (F.col("decimalLatitude") <= 90)
    )
    .filter(
        (F.col("decimalLongitude") >= -180)
        & (F.col("decimalLongitude") <= 180)
    )
    .select(
        "gbifID",
        "scientificName",
        "japanese_name",
        "query_scientific_name",
        "country",
        "countryCode",
        "stateProvince",
        "county",
        "municipality",
        "locality",
        "decimalLatitude",
        "decimalLongitude",
        "coordinateUncertaintyInMeters",
        "eventDate",
        "event_year",
        "event_month",
        "event_day",
    )
)

display(geographic_observations_df)


# COMMAND ----------

# DBTITLE 1,Create Geographic Grid
# 緯度経度を0.1度単位に丸め、
# 近接する観察地点をある程度まとめる
geographic_summary_df = (
    geographic_observations_df
    .withColumn(
        "latitude_grid",
        F.round(
            F.col("decimalLatitude"),
            1,
        ),
    )
    .withColumn(
        "longitude_grid",
        F.round(
            F.col("decimalLongitude"),
            1,
        ),
    )
    .groupBy(
        "latitude_grid",
        "longitude_grid",
        "country",
        "countryCode",
        "stateProvince",
        "county",
        "municipality",
        "query_scientific_name",
        "japanese_name",
    )
    .agg(
        F.count("*").alias("observations"),
    )
    .orderBy(
        "query_scientific_name",
        F.desc("observations"),
    )
)

display(geographic_summary_df)


# COMMAND ----------

# DBTITLE 1,Write Geographic Summary to Gold
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