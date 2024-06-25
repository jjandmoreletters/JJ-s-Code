# Databricks notebook source
# MAGIC %md
# MAGIC ### Conformed Transform - dimTemp
# MAGIC ####Overview
# MAGIC | Detail | Info |
# MAGIC |--------|------|
# MAGIC | Author | Jonathan Martin (JonathanJMartin7@hotmail.co.uk) |
# MAGIC | Ref.   | Add documentation link | 
# MAGIC | Input  | cleaned<li> Temp1 (not used)  </li> <li> Temp2 </li>  
# MAGIC | Output | conformed<li> dimTemp </li>
# MAGIC | Parameters | Debug, Load Identifier
# MAGIC
# MAGIC ####Version History
# MAGIC | Date   | Developed By | Reason|
# MAGIC |--------|--------------|-------|
# MAGIC | 20-Jun-24 | Jonathan Martin | Initial Creation + Testing
# MAGIC
# MAGIC ####Description
# MAGIC Add brief description of transform
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime, timedelta, timezone
import dateutil.parser


# COMMAND ----------

# DBTITLE 1,Run pre scripts
# MAGIC %run "/Workspace/helpers/pre_scripts"

# COMMAND ----------

# DBTITLE 1,Notebook parameters
# debug flag used for verbose logging
dbutils.widgets.text("Debug", "False")

# LoadIdentifier
dbutils.widgets.text("LoadIdentifier", "")

# COMMAND ----------

# DBTITLE 1,Feed specific variables
# the prepared delta table name
conformed_delta_table_name = "conformed.dimTemp"

# the delta table path in the "prepared" layer
conformed_delta_table_filepath = "/dimTemp"

# primary keys => used to check primary keys and log pk consistency
# N.B. use the TRANSFORMED column names
primary_keys = ["TempSkId"]

# for data quality logging purpose
cleaned_data_asset = "cleaned.Temp_temp_file_name"
cleaned_delta_table_filepath = "/temp/Temp_temp_file_name"
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

# Assign Load Identifier
load_identifier = dbutils.widgets.get("LoadIdentifier")

if load_identifier == "":
      load_identifier = datetime.now().strftime("%Y%m%d%H%M%S")

    
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

# COMMAND ----------

# DBTITLE 1,Start processing message
start_message = f"Processing {conformed_delta_table_name}"
if debug:
    print (start_message)
log_trace_info(start_message)

# COMMAND ----------

# DBTITLE 1,Create conformed delta table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {conformed_delta_table_name}
(
  TempSkid Bigint GENERATED ALWAYS AS IDENTITY
 ,TempEkid Integer
 ,TempObjectID Varchar(20)
 ,TempID Varchar(100)
 ,TempName Varchar(250)
 ,TempDescription Varchar(150)
 ,CurrentTempObjectID Varchar(50)
 ,DataDate Timestamp
 --default column file names
 ,ValidFromDt Timestamp
 ,ValidToDt Timestamp
 ,CurrInd Boolean
 ,InsertCheckSum Bigint
 ,UpdateCheckSum Bigint
 ,RecordCreatedDt Timestamp
 ,RecordUpdatedDt Timestamp
 ,SourceSystemCd Varchar(10)
 ,LoadID Varchar(100)
)
USING DELTA
LOCATION '{conformed_path}{conformed_delta_table_filepath}'
""")

# for logging purposes
df = spark.sql(f"""SELECT * FROM {conformed_delta_table_name} LIMIT 10""")
schema = df.schema

# COMMAND ----------

# DBTITLE 1,Selecting the Maximum Value for Business Key Id
#Fetch Maximum TempEkId in order to maintain the sequence and uniqueness for the TempEkId
Max_TempEkId = spark.sql("SELECT nvl(max(TempEkId),0) FROM conformed.dimTemp").collect()[0][0]
print(Max_TempEkId)

# COMMAND ----------

# DBTITLE 1,Create an source view of the incoming data: transformations, joins, renames

df_Source_View = spark.sql(f"""
	SELECT  Temp.ObjectId AS TempObjectID
 		   ,Temp.ID AS TempID
		   ,Temp.Name AS  TempName
           ,Temp.Description AS  TempDescription
           ,Temp.CurrentTempObjectID
           ,Temp.DataDate
           ,Temp2.TempCodeValue AS TempType
           ,Temp2.TempCodeDescription AS TempTypeDesc
	FROM cleaned.Temp_temp_file_name Temp
	LEFT JOIN cleaned.Temp_temp_file_name_2 Temp2 ON Temp.ObjectId = Temp2.Temp2ObjectID AND Temp2.Temp2Name = 'filtered Name of column'
	""")

#WHERE Temp.BusinessAreaKey = 1
# Adding Checksum for SCD2 row comparision of the business columns
df_Source_View = df_Source_View.withColumn("CheckSum", xxhash64(*df_Source_View.schema.names))

#Create Temporary View
df_Source_View.createOrReplaceTempView('__SourceView')



# COMMAND ----------

# DBTITLE 1,Create __UpdateView with rows to be Inserted/Updated based on Checksum comparison
_sqldf = spark.sql(f"""
CREATE OR REPLACE TEMP VIEW __UpdateView AS 
(                   
   SELECT 
      src.TempObjectID
      ,src.TempID
      ,src.TempName
      ,src.TempDescription
      ,src.CurrentTempObjectID
      ,src.DataDate
      ,src.TempType
      ,src.TempTypeDesc
      ,CASE 
           WHEN dimTemp.TempEkId IS NULL THEN 'Insert'
           WHEN dimTemp.TempEkId IS NOT NULL AND src.CheckSum  <> dimTemp.UpdateCheckSum THEN 'Update'
           ELSE 'None'
       END AS RowAction
    ,dimTemp.TempEkId
    ,CURRENT_TIMESTAMP() AS ValidFromDt
    ,CAST('9999-12-31T23:59:59' AS Timestamp) AS ValidToDt
    ,True AS CurrInd
    ,'P6' AS SourceSystemCd
    ,nvl(dimTemp.RecordCreatedDt,CURRENT_TIMESTAMP()) AS RecordCreatedDt
    ,CURRENT_TIMESTAMP() AS RecordUpdatedDt
    ,nvl(dimTemp.InsertCheckSum,src.CheckSum) AS InsertCheckSum
    ,src.CheckSum AS UpdateCheckSum
    ,{load_identifier} AS LoadId
    ,dimTemp.LoadId AS Previous_LoadId
  FROM __SourceView src
  LEFT JOIN conformed.dimTemp AS dimTemp ON src.TempObjectID = dimTemp.TempObjectID AND dimTemp.CurrInd = true
)
""")

# COMMAND ----------

# DBTITLE 1,Insert new records for rows for which Business Key does not exist in Target
_sqldf = spark.sql(f"""
    INSERT INTO {conformed_delta_table_name}
    ( 
      TempEkId
      ,TempObjectID
      ,TempID
      ,TempName
      ,TempDescription
      ,CurrentTempObjectID
      ,DataDate
      ,TempType
      ,TempTypeDesc
      --defaulted
      ,ValidFromDt
      ,ValidToDt
      ,CurrInd
      ,InsertCheckSum
      ,UpdateCheckSum
      ,RecordCreatedDt
      ,RecordUpdatedDt
      ,SourceSystemCd
      ,LoadID
      )
SELECT 
      row_number() OVER(PARTITION BY 1 ORDER BY TempObjectID) + {Max_TempEkId} AS TempEkId
      ,TempObjectID
      ,TempID
      ,TempName
      ,TempDescription
      ,CurrentTempObjectID
      ,DataDate
      ,TempType
      ,TempTypeDesc
      ,ValidFromDt
      ,ValidToDt
      ,CurrInd
      ,InsertCheckSum 
      ,UpdateCheckSum 
      ,RecordCreatedDt
      ,RecordUpdatedDt
      ,SourceSystemCd
      ,LoadId  
FROM __UpdateView
WHERE RowAction = 'Insert'     
""")

# COMMAND ----------

# DBTITLE 1,Create a View with rows that need to be Updated and a New version Inserted
_sqldf = spark.sql(f"""
CREATE OR REPLACE TEMP VIEW __MergeView AS 
(                   
   SELECT 
       TempEkId
      ,TempObjectID
      ,TempID
      ,TempName
      ,TempDescription
      ,CurrentTempObjectID
      ,DataDate
      ,TempType
      ,TempTypeDesc
      ,ValidFromDt
      ,ValidToDt
      ,CurrInd
      ,InsertCheckSum 
      ,UpdateCheckSum 
      ,RecordCreatedDt
      ,RecordUpdatedDt
      ,SourceSystemCd
      ,LoadId  
   FROM __UpdateView upd
   WHERE RowAction = 'Update'
    -- this WHERE clause is to ensure this statement can run more than once with the same result!
   AND NOT EXISTS (SELECT 1 FROM {conformed_delta_table_name} tgt WHERE upd.TempEkId = tgt.TempEkId AND upd.LoadId = tgt.LoadId)
   UNION
   SELECT
       TempEkId
      ,TempObjectID
      ,TempID
      ,TempName
      ,TempDescription
      ,CurrentTempObjectID
      ,DataDate
      ,TempType
      ,TempTypeDesc
      ,ValidFromDt
      ,ValidToDt
      ,CurrInd
      ,InsertCheckSum 
      ,UpdateCheckSum 
      ,RecordCreatedDt
      ,RecordUpdatedDt
      ,SourceSystemCd
      ,Previous_LoadId AS LoadId
   FROM __UpdateView
   WHERE RowAction = 'Update'
)
""")

# COMMAND ----------

# DBTITLE 1,Merge - Insert new record with changes and update previous record for SCD2
_sqldf = spark.sql(f"""
    MERGE INTO {conformed_delta_table_name} target USING __MergeView src
    -- match on the business key
    ON target.TempEkId = src.TempEkId AND target.LoadId = src.LoadId
    WHEN MATCHED THEN 
    UPDATE SET
         target.ValidToDt = try_subtract(src.ValidFromDt, INTERVAL '1' MINUTE)
        ,target.CurrInd = False
    WHEN NOT MATCHED THEN
      INSERT  
      (
      TempEkId
      ,TempObjectID
      ,TempID
      ,TempName
      ,TempDescription
      ,CurrentTempObjectID
      ,DataDate
      ,TempType
      ,TempTypeDesc
      ,ValidFromDt
      ,ValidToDt
      ,CurrInd
      ,InsertCheckSum
      ,UpdateCheckSum
      ,RecordCreatedDt
      ,RecordUpdatedDt
      ,SourceSystemCd
      ,LoadID
      )
      VALUES 
      ( 
       src.TempEkid
      ,src.TempObjectID
      ,src.TempID
      ,src.TempName
      ,src.TempDescription
      ,src.CurrentTempObjectID
      ,src.DataDate
      ,src.TempType
      ,src.TempTypeDesc
      ,src.ValidFromDt
      ,src.ValidToDt
      ,src.CurrInd
      ,src.InsertCheckSum
      ,src.UpdateCheckSum
      ,src.RecordCreatedDt
      ,src.RecordUpdatedDt
      ,src.SourceSystemCd
      ,src.LoadID
      )
""")

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
target_count_df = spark.sql(f"""SELECT COUNT(*) cn FROM {conformed_delta_table_name} WHERE CurrInd = true """)

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
# MAGIC select count(*)  from conformed.dimTemp

# COMMAND ----------

# MAGIC %sql
# MAGIC select *  from conformed.dimTemp limit 10

# COMMAND ----------

#spark.sql(f"DROP TABLE {conformed_delta_table_name}")
#dbutils.fs.rm(f"{conformed_path}{conformed_delta_table_filepath}", True)
