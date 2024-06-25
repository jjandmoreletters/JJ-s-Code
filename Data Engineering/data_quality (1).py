# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS dataquality

# COMMAND ----------

# dataquality.SchemaConsistency
# Schema Consistency = [Number of records that comply with the given schema and nullability constraints]/[Total number of records] %
spark.sql(f"""
CREATE TABLE IF NOT EXISTS dataquality.dqlog(
  DqCheckDate date
  ,DqCheckType string
  ,NotebookName string
  ,SourceDataAsset string
  ,SourceDataAssetPath string
  ,TargetDataAsset string
  ,TargetDataAssetPath string
  ,SchemaReference string
  ,FirstRecordMeasureTitle string
  ,FirstRecordMeasureValue bigint
  ,SecondRecordMeasureTitle string
  ,SecondRecordMeasureValue bigint
  ,PrimaryKeyChecked string
  ,DataAssetFilesChecked string
  ,__CreatedOn timestamp
  ,__UpdatedOn timestamp

)
USING DELTA
PARTITIONED BY (SourceDataAsset)
LOCATION \'{cleaned_path}/dataquality/dqlog\'
""")
