# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
from pyspark.sql import functions as F

# COMMAND ----------

def add_event_date_parts(df):
    return (
        df
        .withColumn(
            "event_year",
            F.regexp_extract(
                F.col("eventDate"),
                r"^(\d{4})",
                1
            ).cast("int")
        )
        .withColumn(
            "event_month",
            F.when(
                F.col("eventDate").rlike(r"^\d{4}-\d{2}"),
                F.regexp_extract(
                    F.col("eventDate"),
                    r"^\d{4}-(\d{2})",
                    1
                ).cast("int")
            )
        )
        .withColumn(
            "event_day",
            F.when(
                F.col("eventDate").rlike(r"^\d{4}-\d{2}-\d{2}"),
                F.regexp_extract(
                    F.col("eventDate"),
                    r"^\d{4}-\d{2}-(\d{2})",
                    1
                ).cast("int")
            )
        )
    )

# COMMAND ----------

bronze_df = spark.read.table("workspace.bronze.gbif_occurrences")

# COMMAND ----------

silver_df = bronze_df.select(
    F.get_json_object("raw_json", "$.gbifID").alias("gbifID"),
    F.get_json_object("raw_json", "$.scientificName").alias("scientificName"),
    F.get_json_object("raw_json", "$.species").alias("species"),
    F.get_json_object("raw_json", "$.kingdom").alias("kingdom"),
    F.get_json_object("raw_json", "$.phylum").alias("phylum"),
    F.get_json_object("raw_json", "$.class").alias("class"),
    F.get_json_object("raw_json", "$.order").alias("order"),
    F.get_json_object("raw_json", "$.family").alias("family"),
    F.get_json_object("raw_json", "$.genus").alias("genus"),
    F.get_json_object("raw_json", "$.country").alias("country"),
    F.get_json_object("raw_json", "$.countryCode").alias("countryCode"),
    F.get_json_object("raw_json", "$.decimalLatitude")
        .cast("double")
        .alias("decimalLatitude"),
    F.get_json_object("raw_json", "$.decimalLongitude")
        .cast("double")
        .alias("decimalLongitude"),
    F.get_json_object("raw_json", "$.eventDate").alias("eventDate"),
    F.get_json_object("raw_json", "$.basisOfRecord").alias("basisOfRecord"),
    F.get_json_object("raw_json", "$.occurrenceStatus")
        .alias("occurrenceStatus"),
    F.col("query_scientific_name"),
    F.col("ingested_at"),
)

silver_df = add_event_date_parts(silver_df)

# COMMAND ----------

quality_df = silver_df.select(
    F.count("*").alias("total_records"),
    F.sum(
        F.when(F.col("gbifID").isNull(), 1).otherwise(0)
    ).alias("missing_gbifID"),
    F.sum(
        F.when(F.col("scientificName").isNull(), 1).otherwise(0)
    ).alias("missing_scientificName"),
    F.sum(
        F.when(F.col("countryCode").isNull(), 1).otherwise(0)
    ).alias("missing_countryCode"),
    F.sum(
        F.when(
            (F.col("decimalLatitude") < -90)
            | (F.col("decimalLatitude") > 90),
            1,
        ).otherwise(0)
    ).alias("invalid_latitude"),
    F.sum(
        F.when(
            (F.col("decimalLongitude") < -180)
            | (F.col("decimalLongitude") > 180),
            1,
        ).otherwise(0)
    ).alias("invalid_longitude"),
)

display(quality_df)

# COMMAND ----------

silver_table = "workspace.silver.gbif_occurrences"

(
    silver_df.write
    .format("delta")
    .option("mergeSchema", "true")
    .mode("overwrite")
    .saveAsTable(silver_table)
)

print(
    f"Silver table updated: {silver_table}, "
    f"records={silver_df.count()}"
)