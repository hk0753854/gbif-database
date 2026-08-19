# Databricks notebook source
species_master_file = "/Workspace/Users/newstatus.vinhoverde@gmail.com/animal-database/src/input_files/species_master.csv"

# COMMAND ----------

species_master = spark.read.csv(species_master_file, encoding="utf-8", header=True)

# COMMAND ----------

species_master.write.format("delta").mode("overwrite").saveAsTable("workspace.mstr.species_master")

# COMMAND ----------

display(species_master)