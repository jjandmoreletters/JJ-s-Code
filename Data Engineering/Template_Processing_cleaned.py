# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime, timedelta, timezone

# COMMAND ----------

# MAGIC %run "/Workspace/helpers/pre_scripts"

# COMMAND ----------

# P6 File Name 
dbutils.widgets.text("TempFileName","")

#start time for the processing window
dbutils.widgets.text("WindowStartTime","")

# end time for the processing window
dbutils.widgets.text("WindowEndTime","")

# debug flag used for verbose logging
dbutils.widgets.text("Debug","False")

# COMMAND ----------

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

temp_file_name = dbutils.widgets.get("TempFileName")

window_start_time = dbutils.widgets.get("WindowStartTime")
if window_start_time=="":
      raise Exception("No WindowStartTime provided, please provide a value for WindowStartTime in this format (py syntax): %Y-%m-%dT%H:%M:%SZ")
        
window_start_time = datetime.strptime(f"{window_start_time[:-2]}Z", "%Y-%m-%dT%H:%M:%S.%fZ")

year = window_start_time.strftime("%Y")
month = window_start_time.strftime("%m")
day = window_start_time.strftime("%d")

window_end_time = dbutils.widgets.get("WindowEndTime")
#If running 1 day, leave WindowEndTime blank. For multi day runs enter the end datetime
if window_end_time == "":
      window_end_time = window_start_time + timedelta(days=1)
else:
      window_end_time = datetime.strptime(f"{window_end_time[:-2]}Z", "%Y-%m-%dT%H:%M:%S.%fZ")

window_start_time_message = "window_start_time: {}".format(window_start_time.strftime("%Y-%m-%dT%H:%M:%SZ"))
window_end_time_message = "window_end_time: {}".format(window_end_time.strftime("%Y-%m-%dT%H:%M:%SZ"))

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        
if debug:
    print (window_start_time_message)
    print (window_end_time_message)
    
log_trace_info(window_start_time_message)
log_trace_info(window_end_time_message)

# COMMAND ----------

# Read Metadata values
df_metadata = spark.read.format("csv") \
   .option("header", "true") \
   .load("file:/Workspace/metadata/Temp_Ingestion_Metadata.csv")\
       .filter(col("FileName") == temp_file_name) 

df_primary_keys =  (df_metadata.select('ColumnName').where(df_metadata.PrimaryKey == 'true'))     
primary_key_columns = df_primary_keys.select('ColumnName').collect()        

# COMMAND ----------


# ingestion_feed_path => used to find the data in raw
ingestion_feed_path = "/temp/" + temp_file_name

# raw_feed_path => used to read data from raw
raw_feed_path = raw_path + ingestion_feed_path

# cleaned_delta_table_name => used to derive the cleaned hive metastore table name
cleaned_delta_table_name = "cleaned.Temp_" + temp_file_name

# cleaned_delta_table_filepath => used to derive the cleaned path
cleaned_delta_table_filepath = "/temp/Temp_"+ temp_file_name

# primary keys => used to check primary keys and log pk consistency
#Assign Primary Key columns
primary_keys = []
for i in primary_key_columns:
    primary_keys.append(i[0])


#Generate Schema definition from metadata
# schema => used to check schema compliance and log schema consistency
schema = 'StructType(['
for row in df_metadata.rdd.collect():
    schema  = schema + 'StructField("' + row.ColumnName + '",' + row.DataType + 'Type(), True),'

schema = schema + 'StructField("__corrupt_record", StringType(), True),' 
schema = schema + 'StructField("year", StringType(), False),'
schema = schema + 'StructField("month", StringType(), False),'
schema = schema + 'StructField("day", StringType(), False)'
schema = schema + '])'

#Convert String to Struct
schema = eval(schema)
# source_data_asset, target_data_asset => used in data quality monitoring
raw_data_asset = "raw" + ingestion_feed_path.replace("/", ".")
cleaned_data_asset = cleaned_delta_table_name


# COMMAND ----------

# DBTITLE 1,Start processing message
start_message = "Processing {0} to {1}; files at '{2}'".format(raw_data_asset, cleaned_data_asset, raw_feed_path)
if debug:
    print (start_message)
log_trace_info(start_message)

# COMMAND ----------

# DBTITLE 1,Read data 
df = spark.read\
    .format("csv")\
    .option("delimiter", "|")\
    .option("header", "true")\
    .option("multiline","true")\
    .option("columnNameOfCorruptRecord", "__corrupt_record")\
    .load(raw_feed_path, schema=schema, mode="PERMISSIVE")\
    .withColumn("__WindowStartTime", lit(window_start_time))\
    .withColumn("__WindowEndTime", lit(window_end_time))\
    .withColumn("__RawPartitionDate", to_date(concat(col("year"), lit("-"), col("month"), lit("-"), col("day"))))\
                                 .filter((col("__RawPartitionDate") >= window_start_time.date()) & (col("__RawPartitionDate") < window_end_time.date()))\
    .withColumn("__RawFileName", input_file_name())


#display(df)

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
#spark.catalog.clearCache()
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
    log_exception(error_message)
    raise Exception(error_message)

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

#Assign values to variables for using dynamic SQL

vColumnList = ''
vCreateColumnList = ''
vInsertColumnList=''
vInsertValuesList = ''
vMergeColumnList = ''

#Get All column names and it's types
for col in df.dtypes:
    if col[0] not in ('__corrupt_record'):
        vColumnList  = vColumnList + ',Df.' + col[0]
        vCreateColumnList  = vCreateColumnList + ',' + col[0] + ' ' + col[1] 
        vInsertColumnList  = vInsertColumnList + ',' + col[0]
        vInsertValuesList  = 'src,' + vInsertValuesList + ',' + col[0]
        vMergeColumnList   = vMergeColumnList + ',tgt.' + col[0] + ' = src.' + col[0] + '\n'

vColumnList = vColumnList [1:]
vCreateColumnList = vCreateColumnList [1:]
vInsertColumnList = vInsertColumnList [1:]
vMergeColumnList = vMergeColumnList [1:]

#Create string value for Primary Key List
string_list = [str(element) for element in primary_keys]
delimiter = ", "
vPrimaryKey = delimiter.join(string_list)

#Create string value for Primary Key for Merge Statement
string_list = [('src.' + str(element) + ' = tgt.' + str(element)) for element in primary_keys]
delimiter = " AND "
vMergePrimaryKey = delimiter.join(string_list)


# COMMAND ----------

vNonDupViewCreateStatement = 'CREATE OR REPLACE TEMP VIEW __NonDedupUpdateView AS ('\
+ 'SELECT * FROM (' \
+ 'SELECT *, DENSE_RANK() OVER(PARTITION BY ' + vPrimaryKey + ', ' \
+  '__RawPartitionDate ORDER BY __RawFileModificationTime DESC) AS RankSamePartitionDeduplication FROM(' + ' \n' \
+ ' SELECT ' + vColumnList + ',FileDetailsDf.__RawFileModificationTime AS __RawFileModificationTime ' + '\n' \
+ ' FROM Df ' + '\n' \
+ 'INNER JOIN FileDetailsDf ON Df.__RawFileName = FileDetailsDf.__RawFileName ' + '\n' \
+ 'WHERE Df.__corrupt_record IS NULL))' + '\n'\
+ 'WHERE RankSamePartitionDeduplication = 1 );'

#Check SQL
#print('vNonDupViewCreateStatement: ' + vNonDupViewCreateStatement)

#Execute the SQL
spark.sql(vNonDupViewCreateStatement)



# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW __UpdateView AS
# MAGIC SELECT DISTINCT * FROM __NonDedupUpdateView;

# COMMAND ----------

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

# raise error if there are duplicates
if num_duplicated_records != 0:
    error_message = "There are [{0}] duplicate records in the following file list {1}".format(num_duplicated_records, processing_files_list)
    log_exception(error_message)
    raise Exception(error_message)

# COMMAND ----------

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

# raise error if PK is not respected
if primary_key_check_df.count() != 0:
    error_message = "PK not respected for '{0}' in the following file list {1}".format(ingestion_feed_path, processing_files_list)
    log_exception(error_message)
    raise Exception(error_message)

# COMMAND ----------

vTableCreateStatement = 'CREATE TABLE IF NOT EXISTS ' + cleaned_delta_table_name + ' (' + '\n' \
+ vCreateColumnList + ',__RawFileModificationTime timestamp,__CreatedOn timestamp,__UpdatedOn timestamp)' + '\n' \
+ ' USING DELTA  LOCATION "' +  cleaned_path + cleaned_delta_table_filepath + '"'

#Execute the SQL
spark.sql(vTableCreateStatement)

# COMMAND ----------

vMergeStatement = 'MERGE INTO ' + cleaned_delta_table_name + ' tgt  USING __UpdateView src' + '\n' \
+ 'ON ' + vMergePrimaryKey + '\n' \
+ 'WHEN MATCHED THEN UPDATE SET ' + '\n' \
+ vMergeColumnList  \
+ ',tgt.__RawFileModificationTime = src.__RawFileModificationTime' + '\n' \
+ ',tgt.__UpdatedOn = CURRENT_TIMESTAMP()' + '\n' \
+ 'WHEN NOT MATCHED THEN' + '\n' \
+ 'INSERT ' + '\n' \
+ '(' + vInsertColumnList + ',__RawFileModificationTime,__CreatedOn,__UpdatedOn)' + '\n' \
+ 'VALUES ' + '\n' \
+  '(' + vInsertColumnList + ',__RawFileModificationTime,CURRENT_TIMESTAMP(),CURRENT_TIMESTAMP())'


#Execute the SQL
spark.sql(vMergeStatement)



# COMMAND ----------

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

# raise error if count mismatch between source and target
if num_records_in_source != num_records_in_target:
    error_message = "Count mismatch between {0} and {1} after load - NumRecordsInSource = {2}, NumRecordsInTarget = {3}".format(source_data_asset, target_data_asset, num_records_in_source, num_records_in_target)
    log_exception(error_message)
    raise Exception(error_message)

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {cleaned_delta_table_name} limit 5"))


# COMMAND ----------


