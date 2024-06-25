# Databricks notebook source
def ri_AsiteWorkspace(sourceSQLTable):
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW UnknownAsiteWorkspace AS
        SELECT DISTINCT
          a.Id
        FROM {0} AS a
        LEFT JOIN prepared.AsiteWorkspace AS b
          ON a.Id = b.Id
        WHERE b.Id IS NULL
        """.format(sourceSQLTable)
             )

    spark.sql("""
        MERGE INTO prepared.AsiteWorkspace AS target
        USING UnknownAsiteWorkspace AS source
          ON target.Id = source.Id
        WHEN NOT MATCHED THEN
          INSERT (
          Id,
          Name,
          InstalledApplication,
          Client,
          Status,
          Subscription,
          StartDate,
          _UnknownFlag,
          _CreatedOn,
          _UpdatedOn
          )
          VALUES (
          source.Id
          ,"NA"
          ,"NA"
          ,"NA"
          ,"NA"
          ,"NA"
          ,"1970-01-01"
          ,True
          ,CURRENT_TIMESTAMP()
          ,CURRENT_TIMESTAMP()
          )
    """)

# COMMAND ----------

# sourceSQLTable e.g. conformed.Impact
# sourceColumnName: e.g. SourceRiskId (from conformed.Impact)
def ri_Risk(sourceSQLTable, sourceColumnName):
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW UnknownRisk AS
        SELECT DISTINCT
          a.{1}
        FROM {0} AS a
        LEFT JOIN conformed.Risk AS b
          ON a.{1} = b.{1}
        WHERE b.{1} IS NULL
        """.format(sourceSQLTable)
             )

    spark.sql("""
        MERGE INTO conformed.Risk AS target
        USING UnknownRisk AS source
          ON target.{1} = source.{1}
        WHEN NOT MATCHED THEN
          INSERT (
         -- .. conformed.Risk column names
          )
          VALUES (
          source.{1}
          -- .. default values
          )
    """)
