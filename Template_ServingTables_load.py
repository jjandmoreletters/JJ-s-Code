# Databricks notebook source
# MAGIC %md
# MAGIC ##Feed: All Conformed to Serving 1:1
# MAGIC ##Ingestion: Expecting full extractions daily at cleaned->conformed->Serving
# MAGIC ##Deletions: Expecting no deletions at source

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime, timedelta, timezone

# COMMAND ----------

# MAGIC %run "../../../helpers/mount_storage"

# COMMAND ----------

# MAGIC %run "../../../helpers/sql"

# COMMAND ----------

# DBTITLE 1,Create tables in SQL Server
Table1_statement = """IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Table1' and xtype='U')
    CREATE TABLE Table1  (
   Table1Id                  	           int
  ,Table2ForeignId                       int
  ,Table1Code                            varchar(max)
  ,Table1DataDate                        datetime2
  ,Table1Project                         varchar(max)
  ,__UnknownFlag                         bit
  ,__WindowStartTime                     datetime2
  ,__WindowEndTime                       datetime2
  ,__CleanedPartitionDate                date
  ,__CreatedOn                           datetime2
  ,__UpdatedOn                           datetime2
    )
"""
stmt = sql_connection.createStatement()
stmt.executeUpdate(Table1_statement)
stmt.close()

Table2_statement = """IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Table2' and xtype='U')
    CREATE TABLE Table2  (
   Table2Id                     	       int
  ,ParentTable2Id                        int
  ,Table1ForeignId                       int
  ,Table2Code                            varchar(max)
  ,Table2Name                            varchar(max)
  ,__UnknownFlag                         bit
  ,__WindowStartTime                     datetime2
  ,__WindowEndTime                       datetime2
  ,__CleanedPartitionDate                date
  ,__CreatedOn                           datetime2
  ,__UpdatedOn                           datetime2
    )
"""
stmt = sql_connection.createStatement()
stmt.executeUpdate(Table2_statement)
stmt.close()


# COMMAND ----------

# DBTITLE 1,Data Load into Serving Tables

df_Table1 = spark.sql(f"""SELECT * FROM conformed.Table1 """)

df_Table1.write\
.format("jdbc")\
.option("url", url) \
.option("dbtable", "dbo.Table1") \
.option("accessToken", access_token) \
.option("encrypt", encrypt) \
.option("hostNameInCertificate", host_name_in_certificate) \
.option("connectTimeout","30") \
.option("schemaCheckEnabled","false") \
.option("tableLock","true") \
.mode("overwrite") \
.save()

df_Table2 = spark.sql(f"""SELECT * FROM conformed.Table2 """)

df_Table2.write\
.format("jdbc")\
.option("url", url) \
.option("dbtable", "dbo.Table2") \
.option("accessToken", access_token) \
.option("encrypt", encrypt) \
.option("hostNameInCertificate", host_name_in_certificate) \
.option("connectTimeout","30") \
.option("schemaCheckEnabled","false") \
.option("tableLock","true") \
.mode("overwrite") \
.save()
