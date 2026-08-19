# Databricks notebook source

from pathlib import Path

# Bundle files are synchronized to the workspace.
# Resolve the CSV relative to the current notebook environment.
species_master_file = Path.cwd() / "src" / "input_files" / "species_master.csv"

# COMMAND ----------

species_master = spark.read.csv(
    str(species_master_file),
    encoding="utf-8",
    header=True,
)

# COMMAND ----------

species_master.write.format("delta").mode("overwrite").saveAsTable(
    "workspace.mstr.species_master"
)

# COMMAND ----------

display(species_master)