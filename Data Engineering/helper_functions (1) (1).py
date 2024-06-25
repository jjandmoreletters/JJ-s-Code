# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
import datetime
import random
import string
from applicationinsights import TelemetryClient

# COMMAND ----------

def xml_data_loader(url, root_tag, row_tag, schema = None, continue_on_no_data = None):
    """
    Simple data loader for XML files
    Pass in the URL you want to load XML files from, root node of xml and the data node as rowTag that needs to be parsed and optionally the schema object.
    You can also specify a continue_on_no_data flag if you wish the load to continue if there isn't any data
    
    This method needs spark-xml library. Make sure its installed and available for use 
    
    Example: xml_data_loader(raw_Url, "rootnodename", "datanodename", rawXMLSchema, True)
    """
  
    start_message = "Started Reading File at Path {0}".format(url)
    end_message = "Finished Reading File at Path {0}".format(url)
    error_message = "No File Found at Path {0}".format(url)
    continue_message = "No Data for {0}, continuing anyway".format(url)
  
    try:
        #edp_app_insights_trace(startmessage)
        if schema is not None:
            df = spark.read.format("com.databricks.spark.xml")\
                     .option("rootTag", rootTag)\
                     .option("rowTag",rowTag)\
                     .option("nullValue", "")\
                     .option("timestampFormat", 'ISO_INSTANT')\
                     .load(raw_url, schema=schema)
    
        else:
            df = spark.read.format("com.databricks.spark.xml")\
                .option("rootTag", rootTag)\
                .option("rowTag",rowTag)\
                .option("nullValue", "")\
                .option("timestampFormat", 'ISO_INSTANT')\
                .load(raw_url)
        #edp_app_insights_trace(endmessage)

    except Exception as e:
        if continue_on_no_data == True:
            print(continuemessage)
            #edp_app_insights_trace(continuemessage)
        if schema == None:
            field = [StructField("field1", StringType(), True)]
            schema = StructType(field)
            df = sqlContext.createDataFrame(sc.emptyRDD(), schema)
        else:
            edp_app_insights_exception(error_message+str(e))
            raise
            
    return(df)
