# Databricks notebook source
dbutils.widgets.text(
    "scientific_name",
    "Hynobius nebulosus",
    "Scientific Name",
)

dbutils.widgets.text(
    "limit",
    "10",
    "Record Limit",
)

scientific_name = dbutils.widgets.get("scientific_name")
limit = int(dbutils.widgets.get("limit"))

print(f"Target species: {scientific_name}")
print(f"Record limit: {limit}")

# COMMAND ----------

# DBTITLE 1,eventDate を解析する関数
from pyspark.sql import functions as F


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

# DBTITLE 1,BronzeのJSONをSilver用に変換する
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

from pyspark.sql import functions as F


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

# DBTITLE 1,Silver Delta Tableへ保存
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