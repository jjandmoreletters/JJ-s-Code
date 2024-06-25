# Databricks notebook source
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled","true")
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite","true")
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact","true")

# COMMAND ----------

# MAGIC %run "./mount_storage"

# COMMAND ----------

# MAGIC %run "./data_quality"

# COMMAND ----------

# MAGIC %run "./opencensus_logger"
