# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
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

# DBTITLE 1,API疎通確認
import requests

url = "https://api.gbif.org/v1/occurrence/search"

params = {
    "scientificName": scientific_name,
    "limit": limit,
}

response = requests.get(
    url,
    params=params,
    timeout=30,
)

response.raise_for_status()

data = response.json()

print(f"Scientific name: {scientific_name}")
print(f"Records retrieved: {len(data['results'])}")

# COMMAND ----------

import json
from datetime import datetime, timezone

from pyspark.sql import Row


ingested_at = datetime.now(timezone.utc)

rows = [
    Row(
        gbif_key=record["key"],
        raw_json=json.dumps(record, ensure_ascii=False),
        query_scientific_name=scientific_name,
        ingested_at=ingested_at,
    )
    for record in data["results"]
]

df_bronze = spark.createDataFrame(rows)

# COMMAND ----------

# DBTITLE 1,Bronze Delta Tableへ保存
bronze_table = "workspace.bronze.gbif_occurrences"

df_bronze.createOrReplaceTempView("gbif_new_records")

spark.sql(f"""
MERGE INTO {bronze_table} AS target
USING gbif_new_records AS source
ON target.gbif_key = source.gbif_key

WHEN NOT MATCHED THEN
  INSERT (
    gbif_key,
    raw_json,
    query_scientific_name,
    ingested_at
  )
  VALUES (
    source.gbif_key,
    source.raw_json,
    source.query_scientific_name,
    source.ingested_at
  )
""")

# COMMAND ----------

display(spark.read.table("workspace.bronze.gbif_occurrences"))