# Databricks notebook source
# MAGIC %md
# MAGIC ##Source Sytsem: n/a
# MAGIC ##Domain: nan
# MAGIC ##Feed: Table1
# MAGIC ##Frequency: Daily
# MAGIC ##Ingestion: Expecting full extraction daily at raw
# MAGIC ##Features: This script will pull data from raw layer to cleaned layer alogn with data validations

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import timedelta, time, datetime
#import datetime


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
dbutils.widgets.text("Debug","False")

# COMMAND ----------

# DBTITLE 1,###TO COMPLETE FOR A NEW FEED: Feed specific variables
# ingestion_feed_path => used to find the data in raw
ingestion_feed_path = "/sourcesystem/table1"

# raw_feed_path => used to read data from raw
raw_feed_path = raw_path + ingestion_feed_path

# cleaned_delta_table_name => used to derive the cleaned hive metastore table name
cleaned_delta_table_name = "cleaned.Table1"

# cleaned_delta_table_filepath => used to derive the cleaned path
cleaned_delta_table_filepath = "/sourcesystem/table1"

# primary keys => used to check primary keys and log pk consistency
# N.B. use the TRANSFORMED column names
primary_keys = ["Table1Id"]

# schema => used to check schema compliance and log schema consistency
schema = StructType([   
    StructField("Table1Date", TimestampType(), False),
    StructField("Id", StringType(), False),
    StructField("Name", StringType(), False),
    StructField("ObjectId", StringType(), False),
    StructField("Table2Id", StringType(), False),
    StructField("__corrupt_record", StringType(), True),
    StructField("year", StringType(), True),
    StructField("month", StringType(), True),
    StructField("day", StringType(), True)
])

# source_data_asset, target_data_asset => used in data quality monitoring
raw_data_asset = "raw" + ingestion_feed_path.replace("/", ".")
cleaned_data_asset = cleaned_delta_table_name

# COMMAND ----------

# DBTITLE 1,Notebook Parameters checks
debug = dbutils.widgets.get("Debug")
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

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
start_message = "Processing {0} to {1}; files at '{2}'".format(raw_data_asset, cleaned_data_asset, raw_feed_path)
if debug:
    print (start_message)
log_trace_info(start_message)

# COMMAND ----------

# DBTITLE 1,###TO COMPLETE FOR A NEW FEED - Read data 
df = spark.read\
    .format("json")\
    .option("columnNameOfCorruptRecord", "__corrupt_record")\
    .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSXXX")\
    .load(raw_feed_path, schema=schema, mode="PERMISSIVE")\
    .withColumn("__WindowStartTime", lit(window_start_time))\
    .withColumn("__WindowEndTime", lit(window_end_time))\
    .withColumn("__RawPartitionDate", to_date(concat(col("year"), lit("-"), col("month"), lit("-"), col("day"))))\
                                 .filter((col("__RawPartitionDate") >= window_start_time.date()) & (col("__RawPartitionDate") < window_end_time.date()))\
    .withColumn("__RawFileName", input_file_name())

display(df)

# COMMAND ----------

# DBTITLE 1,Get files to process details for logging
processing_files_list = df.select('__RawFileName').distinct().rdd.map(lambda x : x[0]).collect()
processing_files_message = "Attempting to process the following files [\"{0}\"]".format('", "'.join(processing_files_list))
if debug:
    print (processing_files_message)
log_trace_info(processing_files_message)

# COMMAND ----------

# DBTITLE 1,Data Quality Check: Schema Consistency
# check how many records are schema compliant
spark.catalog.clearCache()
df.cache()
num_records_compliant = df.where(col("__corrupt_record").isNull()).count()
if num_records_compliant is None:
    num_records_compliant = 0
num_records_non_compliant = df.where(col("__corrupt_record").isNotNull()).count()
if num_records_non_compliant is None:
    num_records_non_compliant = 0
    
if debug:
    print ("num_records_compliant: {}".format(num_records_compliant))
    print ("num_records_non_compliant: {}".format(num_records_non_compliant))
           
spark.sql(f"""
INSERT INTO dataquality.dqlog\
(DqCheckDate, DqCheckType, NotebookName, SourceDataAsset, SourceDataAssetPath, TargetDataAsset, TargetDataAssetPath, SchemaReference, FirstRecordMeasureTitle, FirstRecordMeasureValue, SecondRecordMeasureTitle, SecondRecordMeasureValue, PrimaryKeyChecked, DataAssetFilesChecked, __CreatedOn, __UpdatedOn)\
VALUES (CURRENT_DATE(), "SchemaConsistency", \"{notebook_path}\", \"{raw_data_asset}\", \"{raw_feed_path}\", "", "", \"{schema}\", "NumRecordsCompliant",{num_records_compliant}, "NumRecordsNonCompliant",{num_records_non_compliant}, "", \"{processing_files_list}\", CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())\
""")

# COMMAND ----------

# DBTITLE 1,We are choosing to fail this notebook if the there is even one non-schema-compliant record
# raise error if schema is not respected even for one record
if num_records_non_compliant != 0:
    error_message = "There are [{0}] non-schema-conforming records in the following file list {1}".format(num_records_non_compliant, processing_files_list)
    edp_app_insights_exception(error_message)
    raise Exception(error_message)

# COMMAND ----------

#spark.sql(f""" DROP TABLE IF EXISTS {cleaned_delta_table_name}""")
#dbutils.fs.rm(f"{cleaned_path}{cleaned_delta_table_filepath}", True)

# COMMAND ----------

# DBTITLE 1,Get modified time of files to process - this will be used to deduplicate
dataframe_input_files = df.select(col("__RawFileName")).distinct().rdd.flatMap(lambda x: x).collect()

dataframe_input_files_list_with_modification_time = []

for input_file in dataframe_input_files:
    t = (input_file, datetime.fromtimestamp(dbutils.fs.ls(input_file)[0].modificationTime/1000))
    dataframe_input_files_list_with_modification_time.append(t)

dataframe_input_file_details_schema = StructType([       
    StructField('__RawFileName', StringType(), False),
    StructField('__RawFileModificationTime', TimestampType(), False)
])

file_details_df = spark.createDataFrame(data=dataframe_input_files_list_with_modification_time, schema=dataframe_input_file_details_schema)

# COMMAND ----------

# DBTITLE 1,Create SQL View from Dataframes
df.createOrReplaceTempView("Df") # the actual data
file_details_df.createOrReplaceTempView("FileDetailsDf") # this is a technical view that will be used to deduplicate

# COMMAND ----------

# DBTITLE 1,###TO COMPLETE FOR A NEW FEED - Conformed transformations: renames, casts, deduplications 
# MAGIC %sql
# MAGIC -- this view is made to cope with data extracted multiple times in a day
# MAGIC -- this view doesn't consider corrupted records
# MAGIC CREATE OR REPLACE TEMP VIEW __NonDedupUpdateView AS (
# MAGIC  SELECT * FROM (
# MAGIC       SELECT *, DENSE_RANK() OVER(PARTITION BY Table1Id, __RawPartitionDate ORDER BY __RawFileModificationTime DESC) AS RankSamePartitionDeduplication FROM(
# MAGIC         SELECT
# MAGIC         Df.Table1Date AS Table1DataDate
# MAGIC           ,TRIM(Df.Id) AS Table1Code
# MAGIC           ,TRIM(Df.Name) AS Table1Name
# MAGIC           ,TRIM(Df.ObjectId) AS Table1Id
# MAGIC           ,TRIM(Df.Table2Id) AS Table2ForeignId 
# MAGIC           ,Df.__WindowStartTime AS __WindowStartTime
# MAGIC           ,Df.__WindowEndTime AS __WindowEndTime
# MAGIC           ,Df.__RawPartitionDate AS __RawPartitionDate
# MAGIC           ,Df.__RawFileName AS __RawFileName
# MAGIC           ,FileDetailsDf.__RawFileModificationTime AS __RawFileModificationTime
# MAGIC           ,Df.year
# MAGIC           ,Df.month
# MAGIC           ,Df.day
# MAGIC         FROM Df
# MAGIC         INNER JOIN FileDetailsDf ON Df.__RawFileName = FileDetailsDf.__RawFileName
# MAGIC         WHERE Df.__corrupt_record IS NULL
# MAGIC       )
# MAGIC     ) WHERE RankSamePartitionDeduplication = 1   
# MAGIC );
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW __UpdateView AS
# MAGIC SELECT DISTINCT * FROM __NonDedupUpdateView;

# COMMAND ----------

# DBTITLE 1,Data Quality Check: Record Duplication
__update_view_df = spark.sql(f"""SELECT * FROM __UpdateView""")
__non_dedup_update_view_df = spark.sql(f"""SELECT * FROM __NonDedupUpdateView""")

num_total_records = __non_dedup_update_view_df.count()
if num_total_records is None:
    num_total_records = 0
    
num_distinct_records = __update_view_df.count()
if num_distinct_records is None:
    num_distinct_records = 0
    
num_duplicated_records = num_total_records - num_distinct_records

if debug:
    print ("num_total_records: {}".format(num_total_records))
    print ("num_distinct_records: {}".format(num_distinct_records))

# log in Duplication check
spark.sql(f"""
INSERT INTO dataquality.dqlog\
(DqCheckDate, DqCheckType, NotebookName, SourceDataAsset, SourceDataAssetPath, TargetDataAsset, TargetDataAssetPath, SchemaReference, FirstRecordMeasureTitle, FirstRecordMeasureValue, SecondRecordMeasureTitle, SecondRecordMeasureValue, PrimaryKeyChecked, DataAssetFilesChecked, __CreatedOn, __UpdatedOn)\
VALUES (CURRENT_DATE(), "Duplication", \"{notebook_path}\", \"{raw_data_asset}\", \"{raw_feed_path}\", "", "", \"{schema}\", "NumTotalRecords", {num_total_records}, "NumDistinctRecords",{num_distinct_records}, "", \"{processing_files_list}\", CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())\
""")

# COMMAND ----------

# DBTITLE 1,We are choosing to fail this notebook if there are duplicate records
# raise error if there are duplicates
if num_duplicated_records != 0:
    error_message = "There are [{0}] duplicate records in the following file list {1}".format(num_duplicated_records, processing_files_list)
    edp_app_insights_exception(error_message)
    raise Exception(error_message)

# COMMAND ----------

# DBTITLE 1,Data Quality Check: Primary Key Consistency
# since we are using FAILFAST, if we reach this cell we can assume all the records will be compliant to the schema

# get dynamically the primary key list
primary_key_list = ", ".join(primary_keys)

# count the primary keys we have in the feed
# adding the partition date as a primary key in raw
num_primary_key_df = spark.sql(f"""
    SELECT COUNT(DISTINCT({primary_key_list}, __RawPartitionDate)) NumPrimaryKeys
    FROM __UpdateView
""")
num_primary_keys = num_primary_key_df.agg(sum("NumPrimaryKeys")).collect()[0][0]
if num_primary_keys is None:
    num_primary_keys = 0

# check for duplicate primary keys
# adding the partition date as a primary key in raw
primary_key_check_df = spark.sql(f"""
SELECT {primary_key_list}, __RawPartitionDate, COUNT(*) NumRow
FROM __UpdateView
GROUP BY {primary_key_list}, __RawPartitionDate
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
VALUES (CURRENT_DATE(), "PrimaryKeyConsistency", \"{notebook_path}\", \"{raw_data_asset}\", \"{raw_feed_path}\", "", "", \"{schema}\", "NumDuplicatedPrimaryKeys", {num_duplicated_primary_keys}, "NumPrimaryKeys", {num_primary_keys}, \"{primary_key_list}\", \"{processing_files_list}\", CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())\
""")

# COMMAND ----------

# DBTITLE 1,We are choosing to fail this notebook if the PK is not respected
# raise error if PK is not respected
if primary_key_check_df.count() != 0:
    error_message = "PK not respected for '{0}' in the following file list {1}".format(ingestion_feed_path, processing_files_list)
    edp_app_insights_exception(error_message)
    raise Exception(error_message)

# COMMAND ----------

# DBTITLE 1,###TO COMPLETE FOR A NEW FEED - Create metastore delta table
# we are partitioning the table by year, month, day - like the raw feed
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {cleaned_delta_table_name}(
  Table1Id int NOT NULL
  ,Table2ForeignId int
  ,Table1Code      string
  ,Table1DataDate       timestamp
  ,Table1Name string
  ,__WindowStartTime timestamp
  ,__WindowEndTime timestamp
  ,__RawFileName string
  ,__RawFileModificationTime timestamp
  ,__CreatedOn timestamp
  ,__UpdatedOn timestamp
  ,year string
  ,month string
  ,day string
  ,__RawPartitionDate date
)
USING DELTA
PARTITIONED BY (year, month, day, __RawPartitionDate)
LOCATION \'{cleaned_path}{cleaned_delta_table_filepath}\'
""")

# COMMAND ----------

# DBTITLE 1,###-Alter Table Commands
# spark.sql(f"""
# ALTER TABLE {cleaned_delta_table_name} SET TBLPROPERTIES (
# 'delta.columnMapping.mode' = 'name',
# 'delta.minReaderVersion' = '2',
# 'delta.minWriterVersion' = '5');
# """)

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,###TO COMPLETE FOR A NEW FEED - INSERT OVERWRITE PARTITIONS
# Databricks 11.1LTS has dynamic OVERWRITE support :)
_sqldf = spark.sql(f"""
INSERT OVERWRITE {cleaned_delta_table_name}
(Table1Id
,Table2ForeignId
,Table1Code
,Table1DataDate
  ,Table1Name
  ,__WindowStartTime
  ,__WindowEndTime
  ,__RawFileName
  ,__RawFileModificationTime
  ,__CreatedOn
  ,__UpdatedOn
  ,year
  ,month
  ,day
  ,__RawPartitionDate
)
SELECT CAST(source.Table1Id AS int)
    ,CAST(source.Table2ForeignId AS int)
    ,source.Table1Code
    ,source.Table1DataDate
  ,source.Table1Name
  ,source.__WindowStartTime
  ,source.__WindowEndTime
  ,source.__RawFileName
  ,source.__RawFileModificationTime
  ,CURRENT_TIMESTAMP() --__CreatedOn
  ,CURRENT_TIMESTAMP() --__UpdatedOn
  ,source.year
  ,source.month
  ,source.day
  ,source.__RawPartitionDate
FROM __UpdateView source
""")

# COMMAND ----------

updated_message_text = "INSERT OVERWRITE into {0}".format(cleaned_delta_table_name)
updated_message_properties = {'num_affected_rows': _sqldf.first()['num_affected_rows']\
                               ,'num_inserted_rows': _sqldf.first()['num_inserted_rows']}
if debug:
    print (updated_message_text, " - ", updated_message_properties)
log_trace_info(updated_message_text, updated_message_properties)

# COMMAND ----------

# DBTITLE 1,Data Quality Check: Count Integrity
target_count_df = spark.sql(f"""SELECT COUNT(*) cn FROM {cleaned_delta_table_name} WHERE __RawPartitionDate >= '{window_start_time.date()}' AND __RawPartitionDate < '{window_end_time.date()}'""")

# num_records_in_source = number of records of the __UpdateView (after dropping malformed and deduplicating)
num_records_in_source = num_distinct_records
num_records_in_target = target_count_df.first()['cn']

if debug:
    print ("num_records_in_source: {}".format(num_records_in_source))
    print ("num_records_in_target: {}".format(num_records_in_target))

# log in CountIntegrity check
spark.sql(f"""
INSERT INTO dataquality.dqlog\
(DqCheckDate, DqCheckType, NotebookName, SourceDataAsset, SourceDataAssetPath, TargetDataAsset, TargetDataAssetPath, SchemaReference, FirstRecordMeasureTitle, FirstRecordMeasureValue, SecondRecordMeasureTitle, SecondRecordMeasureValue, PrimaryKeyChecked, DataAssetFilesChecked, __CreatedOn, __UpdatedOn)\
VALUES (CURRENT_DATE(), "CountIntegrity", \"{notebook_path}\", \"{raw_data_asset}\", \"{raw_feed_path}\", \"{cleaned_data_asset}\", \"{cleaned_path}{cleaned_delta_table_filepath}\", "", "NumRecordsInSource", {num_records_in_source}, "NumRecordsInTarget", {num_records_in_target}, "", \"{processing_files_list}\", CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())\
""")

# COMMAND ----------

# DBTITLE 1,We are choosing to fail this notebook if there is a count mismatch between source and target
# raise error if count mismatch between source and target
if num_records_in_source != num_records_in_target:
    error_message = "Count mismatch between {0} and {1} after load - NumRecordsInSource = {2}, NumRecordsInTarget = {3}".format(source_data_asset, target_data_asset, num_records_in_source, num_records_in_target)
    edp_app_insights_exception(error_message)
    raise Exception(error_message)

# COMMAND ----------

end_message = "Successfully processed {0} to {1}".format(raw_data_asset, cleaned_data_asset)
if debug:
    print (end_message)
log_trace_info(end_message)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleaned.Table1 LIMIT 10
