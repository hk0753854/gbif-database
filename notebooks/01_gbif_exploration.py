# Databricks notebook source
import sys

print(sys.executable)

# COMMAND ----------

import duckdb
import pandas as pd
import requests

print("Python environment is ready!")
print(f"pandas: {pd.__version__}")
print(f"duckdb: {duckdb.__version__}")

# COMMAND ----------


url = "https://api.gbif.org/v1/species/match"

params = {
    "name": "Plestiodon japonicus"
}

response = requests.get(
    url,
    params=params,
    timeout=30,
)

print(response.status_code)
print(response.url)
print(response.json())

# COMMAND ----------

data = response.json()

data

# COMMAND ----------

import pandas as pd

df = pd.DataFrame([data])

df

# COMMAND ----------

df.columns.tolist()