# Databricks notebook source
# MAGIC %md
# MAGIC ##Template: Conformed
# MAGIC ##Feed: Resource SCD1
# MAGIC ##Frequency: Daily
# MAGIC ##Ingestion: Expecting full extractions daily at cleaned
# MAGIC ##Deletions: Expecting no deletions at source

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime, timedelta, timezone

# COMMAND ----------

# DBTITLE 1,Run pre scripts
# MAGIC %run "../../../helpers/pre_scripts"

# COMMAND ----------

# DBTITLE 1,Notebook parameters
# start time for the processing window
dbutils.widgets.text("WindowStartTime","")

# end time for the processing window
dbutils.widgets.text("WindowEndTime","")

# debug flag used for verbose logging
dbutils.widgets.text("Debug", "False")

# COMMAND ----------

# DBTITLE 1,Feed specific variables
# the prepared delta table name
conformed_delta_table_name = "conformed.Table1"

# the delta table path in the "prepared" layer
conformed_delta_table_filepath = "/sourcesystem/Table1"

# primary keys => used to check primary keys and log pk consistency
# N.B. use the TRANSFORMED column names
primary_keys = ["Table1Id"]

# for data quality logging purpose
cleaned_data_asset = "cleaned.Table1"
cleaned_delta_table_filepath = "/sourcesystem/Table1"
conformed_data_asset = conformed_delta_table_name

# COMMAND ----------

# DBTITLE 1,Notebook parameters check
# Debug validate
debug = dbutils.widgets.get("Debug")
if debug.lower() == 'true':
    debug = True
elif debug.lower() == 'false':
    debug = False
else:
    exception_message = "Debug must be [True, False]"
    log_exception(exception_message)
    raise Exception(exception_message)
    
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

# window start time, window end time
window_start_time = dbutils.widgets.get("WindowStartTime")
if window_start_time=="":
      raise Exception("No WindowStartTime provided, please provide a value for WindowStartTime in this format (py syntax): %Y-%m-%dT%H:%M:%SZ")
        
window_start_time = datetime.strptime(window_start_time,"%Y-%m-%dT%H:%M:%SZ") 
#window_start = window_start_time.strftime('%Y-%m-%d')

window_end_time = dbutils.widgets.get("WindowEndTime")
#If running 1 day, leave WindowEndTime blank. For multi day runs enter the end datetime
if window_end_time == "":
      window_end_time = window_start_time + timedelta(days=1)
else:
      window_end_time = datetime.strptime(window_end_time,"%Y-%m-%dT%H:%M:%SZ")

window_start_time_message = "window_start_time: {}".format(window_start_time.strftime("%Y-%m-%dT%H:%M:%SZ"))
window_end_time_message = "window_end_time: {}".format(window_end_time.strftime("%Y-%m-%dT%H:%M:%SZ"))

if debug:
    print (window_start_time_message)
    print (window_end_time_message)
    
log_trace_info(window_start_time_message)
log_trace_info(window_end_time_message)

# COMMAND ----------

# DBTITLE 1,Start processing message
start_message = f"Processing {conformed_delta_table_name}"
if debug:
    print (start_message)
log_trace_info(start_message)

# COMMAND ----------

#spark.sql(f"DROP TABLE {conformed_delta_table_name}")
#dbutils.fs.rm(f"{conformed_path}{conformed_delta_table_filepath}", True)

# COMMAND ----------

# DBTITLE 1,Create conformed delta table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {conformed_delta_table_name}(
   Table1Id int
    ,Table2ForeignId int
    ,Table1Code    string
    ,Table1DataDate    timestamp
    ,Table1Name    string
  ,__UnknownFlag                         boolean
  ,__WindowStartTime                     timestamp
  ,__WindowEndTime                       timestamp
  ,__CleanedPartitionDate                date
  ,__CreatedOn                           timestamp
  ,__UpdatedOn                           timestamp
)
USING DELTA
LOCATION \'{conformed_path}{conformed_delta_table_filepath}\'
""")

# for logging purposes
df = spark.sql(f"""SELECT * FROM {conformed_delta_table_name} LIMIT 10""")
schema = df.schema

# COMMAND ----------

# DBTITLE 1,###-Alter Tables Command
#spark.sql(f"""
#ALTER TABLE {conformed_delta_table_name} SET TBLPROPERTIES (
#'delta.columnMapping.mode' = 'name',
#'delta.minReaderVersion' = '2',
#'delta.minWriterVersion' = '5');
#""")

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Selecting the latest version for each key in case of loading multiple days, and create an updated view of the incoming data: transformations, joins, renames
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW __UpdateView AS (
  SELECT * FROM (
    SELECT *, DENSE_RANK() OVER(PARTITION BY Table1Id ORDER BY __CleanedPartitionDate DESC) AS RankLatestPartition FROM
    (
	SELECT Table1Id     
    ,Table2ForeignId
    ,Table1Code
    ,Table1DataDate
    ,Table1Name
      ,False AS __UnknownFlag
        ,CAST("{window_start_time.strftime("%Y-%m-%dT%H:%M:%SZ")}" AS TIMESTAMP) AS __WindowStartTime
        ,CAST("{window_end_time.strftime("%Y-%m-%dT%H:%M:%SZ")}" AS TIMESTAMP) AS __WindowEndTime
        ,__RawPartitionDate AS __CleanedPartitionDate
        ,CURRENT_TIMESTAMP() AS __CreatedOn
        ,CURRENT_TIMESTAMP() AS __UpdatedOn
	FROM cleaned.P6Project source
      WHERE  __RawPartitionDate >= '{window_start_time}' AND __RawPartitionDate < '{window_end_time}'
        -- this record will be used for Facts that will have a null value for this table's lookup
      -- in any Fact referencing this Dim, the expression ISNULL(key, "__UnknownProject") will need to be used as the FK value
      UNION ALL
      SELECT 
    -1 as Table1Id 
    ,-1 as Table2ForeignId 
    ,-1 as Table1Code
    ,null as Table1DataDate
    ,null as Table1Name
	        ,True AS __UnknownFlag
        ,CAST("{window_start_time.strftime("%Y-%m-%dT%H:%M:%SZ")}" AS TIMESTAMP) AS __WindowStartTime
        ,CAST("{window_end_time.strftime("%Y-%m-%dT%H:%M:%SZ")}" AS TIMESTAMP) AS __WindowEndTime
        ,null AS __CleanedPartitionDate
        ,CURRENT_TIMESTAMP() AS __CreatedOn
        ,CURRENT_TIMESTAMP() AS __UpdatedOn
        )
    )
  WHERE RankLatestPartition = 1
)
""")

# COMMAND ----------

# DBTITLE 1,Merge records into target table
_sqldf = spark.sql(f"""
    MERGE INTO {conformed_delta_table_name} target USING __UpdateView source
    -- match on the natural key
      ON target.Table1Id = source.Table1Id
      -- specify what columns trigger a versioning of the record in the following "WHEN MATCHED AND" condition
    WHEN MATCHED THEN UPDATE SET
     target.Table2ForeignId = source.Table2ForeignId
    ,target.Table1Name = source.Table1Name
    ,target.Table1Code = source.Table1Code
    ,target.Table1DataDate = source.Table1DataDate
      ,target.__UnknownFlag = source.__UnknownFlag
      ,target.__WindowStartTime = source.__WindowStartTime
      ,target.__WindowEndTime = source.__WindowEndTime
      ,target.__CleanedPartitionDate = source.__CleanedPartitionDate
      ,target.__UpdatedOn = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
      INSERT (Table1Id
    ,Table2ForeignId 
    ,Table1Code
    ,Table1DataDate
    ,Table1Name 
      ,__UnknownFlag
      ,__WindowStartTime
      ,__WindowEndTime
      ,__CleanedPartitionDate
      ,__CreatedOn
      ,__UpdatedOn
      )
      VALUES (source.Table1Id
    ,source.Table2ForeignId 
    ,source.Table1Code
    ,source.Table1DataDate
    ,source.Table1Name  
        ,source.__UnknownFlag
        ,source.__WindowStartTime
        ,source.__WindowEndTime
        ,source.__CleanedPartitionDate
        ,CURRENT_TIMESTAMP()
        ,CURRENT_TIMESTAMP()
      )
""")


# COMMAND ----------

merge_message_text = "MERGE new records and update existing ones from {0}".format(conformed_delta_table_name)
merge_message_properties = {'num_affected_rows': _sqldf.first()['num_affected_rows']\
                               ,'num_updated_rows': _sqldf.first()['num_updated_rows']\
                               ,'num_deleted_rows': _sqldf.first()['num_deleted_rows']\
                               ,'num_inserted_rows': _sqldf.first()['num_inserted_rows']}
if debug:
    print (merge_message_text, " - ", merge_message_properties)
log_trace_info(merge_message_text, merge_message_properties)

# COMMAND ----------

# DBTITLE 1,Data Quality Check: Primary Key Consistency
# get dynamically the primary key list
primary_key_list = ", ".join(primary_keys)

# count the primary keys we have in the feed
# adding the partition date as a primary key in raw
num_primary_key_df = spark.sql(f"""
    SELECT COUNT(DISTINCT({primary_key_list})) NumPrimaryKeys
    FROM {conformed_delta_table_name}
""")
num_primary_keys = num_primary_key_df.agg(sum("NumPrimaryKeys")).collect()[0][0]
if num_primary_keys is None:
    num_primary_keys = 0

# check for duplicate primary keys
# adding the partition date as a primary key in raw
primary_key_check_df = spark.sql(f"""
SELECT {primary_key_list}, COUNT(*) NumRow
FROM {conformed_delta_table_name}
GROUP BY {primary_key_list}
HAVING COUNT(*) > 1
""")

num_duplicated_primary_keys = primary_key_check_df.agg(sum("NumRow")).collect()[0][0]
if num_duplicated_primary_keys is None:
    num_duplicated_primary_keys = 0

if debug:
    print ("primary_key_list: {}".format(primary_key_list))
    print ("num_primary_keys: {}".format(num_primary_keys))
    print ("num_duplicated_primary_keys: {}".format(num_duplicated_primary_keys))

# log in PrimaryKeyConsistency check
spark.sql(f"""
INSERT INTO dataquality.dqlog\
(DqCheckDate, DqCheckType, NotebookName, SourceDataAsset, SourceDataAssetPath, TargetDataAsset, TargetDataAssetPath, SchemaReference, FirstRecordMeasureTitle, FirstRecordMeasureValue, SecondRecordMeasureTitle, SecondRecordMeasureValue, PrimaryKeyChecked, DataAssetFilesChecked, __CreatedOn, __UpdatedOn)\
VALUES (CURRENT_DATE(), "PrimaryKeyConsistency", \"{notebook_path}\", \"{conformed_data_asset}\", \"{conformed_path}{conformed_delta_table_filepath}\", "", "", \"{schema}\", "NumDuplicatedPrimaryKeys", {num_duplicated_primary_keys}, "NumPrimaryKeys", {num_primary_keys}, \"{primary_key_list}\", "", CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())\
""")

# COMMAND ----------

# DBTITLE 1,We are choosing to fail this notebook if the PK is not respected
# raise error if PK is not respected
if primary_key_check_df.count() != 0:
    error_message = "PK not respected for {}".format(conformed_delta_table_name)
    log_exception(error_message)
    raise Exception(error_message)

# COMMAND ----------

# DBTITLE 1,Data Quality Check: Count Integrity
source_count_df = spark.sql(f"""SELECT COUNT(*) cn FROM __UpdateView""")
target_count_df = spark.sql(f"""SELECT COUNT(*) cn FROM {conformed_delta_table_name} WHERE __CleanedPartitionDate IN (SELECT DISTINCT __CleanedPartitionDate FROM __UpdateView) OR __CleanedPartitionDate IS NULL""")

# num_records_in_source = number of records of the __UpdateView (after dropping malformed and deduplicating)
num_records_in_source = source_count_df.first()['cn']
num_records_in_target = target_count_df.first()['cn']

if debug:
    print ("num_records_in_source: {}".format(num_records_in_source))
    print ("num_records_in_target: {}".format(num_records_in_target))

# log in CountIntegrity check
spark.sql(f"""
INSERT INTO dataquality.dqlog\
(DqCheckDate, DqCheckType, NotebookName, SourceDataAsset, SourceDataAssetPath, TargetDataAsset, TargetDataAssetPath, SchemaReference, FirstRecordMeasureTitle, FirstRecordMeasureValue, SecondRecordMeasureTitle, SecondRecordMeasureValue, PrimaryKeyChecked, DataAssetFilesChecked, __CreatedOn, __UpdatedOn)\
VALUES (CURRENT_DATE(), "CountIntegrity", \"{notebook_path}\", \"{cleaned_data_asset}\", \"{cleaned_path}{cleaned_delta_table_filepath}\", \"{conformed_data_asset}\", \"{conformed_path}{conformed_delta_table_filepath}\", "", "NumRecordsInSource", {num_records_in_source}, "NumRecordsInTarget", {num_records_in_target}, "", "", CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())\
""")

# COMMAND ----------

# DBTITLE 1,We are choosing to fail this notebook if there is a count mismatch between source and target
# raise error if count mismatch between source and target
if num_records_in_source != num_records_in_target:
    error_message = "Count mismatch between {0} and {1} after load - NumRecordsInSource = {2}, NumRecordsInTarget = {3}".format(cleaned_data_asset, conformed_data_asset, num_records_in_source, num_records_in_target)
    log_exception(error_message)
    raise Exception(error_message)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM conformed.Table1 Limit 10
