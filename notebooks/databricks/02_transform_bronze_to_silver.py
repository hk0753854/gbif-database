# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Importing PySpark SQL Functions Module
from pyspark.sql import functions as F


# COMMAND ----------

# DBTITLE 1,Extract Year Month and Day from Event Date Column
def add_event_date_parts(df):
    return (
        df.withColumn(
            "event_year",
            F.regexp_extract(
                F.col("eventDate"),
                r"^(\d{4})",
                1,
            ).cast("int"),
        )
        .withColumn(
            "event_month",
            F.when(
                F.col("eventDate").rlike(r"^\d{4}-\d{2}"),
                F.regexp_extract(
                    F.col("eventDate"),
                    r"^\d{4}-(\d{2})",
                    1,
                ).cast("int"),
            ),
        )
        .withColumn(
            "event_day",
            F.when(
                F.col("eventDate").rlike(r"^\d{4}-\d{2}-\d{2}"),
                F.regexp_extract(
                    F.col("eventDate"),
                    r"^\d{4}-\d{2}-(\d{2})",
                    1,
                ).cast("int"),
            ),
        )
    )

# COMMAND ----------

# DBTITLE 1,Load Bronze Table from GBIF Occurrences Dataset
bronze_df = spark.read.table("workspace.bronze.gbif_occurrences")

# COMMAND ----------

# DBTITLE 1,Extract Taxonomy and Geographic Data from Raw JSON
silver_df = bronze_df.select(
    # GBIF identification
    F.get_json_object("raw_json", "$.gbifID").alias("gbifID"),
    F.get_json_object("raw_json", "$.scientificName").alias("scientificName"),
    F.get_json_object("raw_json", "$.species").alias("species"),

    # Taxonomy
    F.get_json_object("raw_json", "$.kingdom").alias("kingdom"),
    F.get_json_object("raw_json", "$.phylum").alias("phylum"),
    F.get_json_object("raw_json", "$.class").alias("class"),
    F.get_json_object("raw_json", "$.order").alias("order"),
    F.get_json_object("raw_json", "$.family").alias("family"),
    F.get_json_object("raw_json", "$.genus").alias("genus"),

    # Geographic information
    F.get_json_object("raw_json", "$.country").alias("country"),
    F.get_json_object("raw_json", "$.countryCode").alias("countryCode"),
    F.get_json_object("raw_json", "$.stateProvince").alias("stateProvince"),
    F.get_json_object("raw_json", "$.county").alias("county"),
    F.get_json_object("raw_json", "$.municipality").alias("municipality"),
    F.get_json_object("raw_json", "$.locality").alias("locality"),
    F.get_json_object("raw_json", "$.decimalLatitude").cast("double").alias("decimalLatitude"),
    F.get_json_object("raw_json", "$.decimalLongitude").cast("double").alias("decimalLongitude"),
    F.get_json_object("raw_json", "$.coordinateUncertaintyInMeters").cast("double").alias("coordinateUncertaintyInMeters"),

    # Observation date
    F.get_json_object("raw_json", "$.eventDate").alias("eventDate"),

    # Observation metadata
    F.get_json_object("raw_json", "$.basisOfRecord").alias("basisOfRecord"),
    F.get_json_object("raw_json", "$.occurrenceStatus").alias("occurrenceStatus"),

    # Pipeline metadata
    F.col("query_scientific_name"),
    F.col("ingested_at"),
)

# COMMAND ----------

# DBTITLE 1,Enhance Silver DataFrame with Event Date Components
silver_df = add_event_date_parts(silver_df)


# COMMAND ----------

# DBTITLE 1,Summarize Missing and Invalid Location and ID Fields
quality_df = silver_df.select(
    F.count("*").alias("total_records"),
    
    # Identification
    F.sum(
        F.when(F.col("gbifID").isNull(), 1)
         .otherwise(0)
    ).alias("missing_gbifID"),
    F.sum(
        F.when(F.col("scientificName").isNull(), 1)
         .otherwise(0)
    ).alias("missing_scientificName"),
    
    # Geographic completeness
    F.sum(
        F.when(F.col("countryCode").isNull(), 1)
         .otherwise(0)
    ).alias("missing_countryCode"),
    F.sum(
        F.when(F.col("stateProvince").isNull(), 1)
         .otherwise(0)
    ).alias("missing_stateProvince"),
    F.sum(
        F.when(F.col("county").isNull(), 1)
         .otherwise(0)
    ).alias("missing_county"),
    F.sum(
        F.when(F.col("municipality").isNull(), 1)
         .otherwise(0)
    ).alias("missing_municipality"),
    F.sum(
        F.when(F.col("locality").isNull(), 1)
         .otherwise(0)
    ).alias("missing_locality"),
    
    # Coordinate completeness
    F.sum(
        F.when(F.col("decimalLatitude").isNull(), 1)
         .otherwise(0)
    ).alias("missing_latitude"),
    F.sum(
        F.when(F.col("decimalLongitude").isNull(), 1)
         .otherwise(0)
    ).alias("missing_longitude"),
    
    # Coordinate validity
    F.sum(
        F.when(
            (F.col("decimalLatitude") < -90) | (F.col("decimalLatitude") > 90),
            1
        ).otherwise(0)
    ).alias("invalid_latitude"),
    F.sum(
        F.when(
            (F.col("decimalLongitude") < -180) | (F.col("decimalLongitude") > 180),
            1
        ).otherwise(0)
    ).alias("invalid_longitude"),
    
    # Coordinate uncertainty
    F.sum(
        F.when(F.col("coordinateUncertaintyInMeters") < 0, 1)
         .otherwise(0)
    ).alias("invalid_coordinate_uncertainty"),
)

# COMMAND ----------

# DBTITLE 1,Display Quality DataFrame for Analysis Results
display(quality_df)


# COMMAND ----------

# DBTITLE 1,Overwrite Silver Table with Delta Format from DataFrame
silver_table = "workspace.silver.gbif_occurrences"

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(silver_table)
)


# COMMAND ----------

# DBTITLE 1,Silver Table Update with Record Count Notification
record_count = silver_df.count()

print(
    f"Silver table updated: {silver_table}, "
    f"records={record_count}"
)