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
import time

import requests

url = "https://api.gbif.org/v1/occurrence/search"

params = {
    "scientificName": scientific_name,
    "limit": limit,
}

max_retries = 3
retry_status_codes = {502, 503, 504}

for attempt in range(1, max_retries + 1):
    try:
        response = requests.get(
            url,
            params=params,
            timeout=30,
        )

        if response.status_code in retry_status_codes:
            raise requests.HTTPError(
                f"Retryable HTTP error: {response.status_code}",
                response=response,
            )

        response.raise_for_status()

        data = response.json()
        break

    except requests.Timeout:
        if attempt == max_retries:
            raise

        wait_seconds = 2**attempt

        print(
            f"Request timed out. "
            f"Retrying in {wait_seconds}s "
            f"(attempt {attempt}/{max_retries})"
        )

        time.sleep(wait_seconds)

    except requests.HTTPError as e:
        if (
            e.response is None
            or e.response.status_code not in retry_status_codes
            or attempt == max_retries
        ):
            raise

        wait_seconds = 2**attempt

        print(
            f"GBIF API returned HTTP {e.response.status_code}. "
            f"Retrying in {wait_seconds}s "
            f"(attempt {attempt}/{max_retries})"
        )

        time.sleep(wait_seconds)

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
from delta.tables import DeltaTable

bronze_table = "workspace.bronze.gbif_occurrences"

target = DeltaTable.forName(spark, bronze_table)

(
    target.alias("target")
    .merge(
        df_bronze.alias("source"),
        "target.gbif_key = source.gbif_key",
    )
    .whenNotMatchedInsertAll()
    .execute()
)

# COMMAND ----------

display(spark.read.table("workspace.bronze.gbif_occurrences"))