# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
username = (
    dbutils.notebook.entry_point
    .getDbutils()
    .notebook()
    .getContext()
    .userName()
    .get()
)

species_master_file = f"/Workspace/Users/{username}/gbif-database/src/input_files/species_master.csv"

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