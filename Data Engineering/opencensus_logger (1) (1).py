# Databricks notebook source
# DBTITLE 1,initialise logger
# for more info on these libraries:
# https://pypi.org/project/opencensus-ext-azure/
# https://docs.python.org/3/library/logging.html

import json
import time
import logging
from datetime import datetime
from opencensus.ext.azure.log_exporter import AzureLogHandler

# requirements: requires application insights's connection string "AppInsightsConnectionString)" secret
app_insights_connection_string = dbutils.secrets.get(scope = "AzureKeyVaultScope", key = "AppInsightsConnectionString")

# initialise logger
logger = logging.getLogger(__name__)
# this is to ensure the AzureLogHandler gets added to the logger just once, otherwise we will have duplicate log entries
# more info here: https://github.com/census-instrumentation/opencensus-python/issues/870
if(not any(isinstance(x, AzureLogHandler) for x in logger.handlers)):
    logger.addHandler(AzureLogHandler(connection_string=app_insights_connection_string))
    print("AzureLogHandler added")
# 20 = INFO
logger.setLevel(level=20)

# get the databricks context and useful notebook properties
databricks_context = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
databricks_properties = {} #{'role': "Databricks"\
                                    #,'host_name': databricks_context['tags']['browserHostName']\
                                    #,'cluster_id': databricks_context['tags']['clusterId']\
                                    #,'notebook_path': databricks_context['extraContext']['notebook_path']\
                                    #,'notebook_cell': databricks_context['tags']['browserHash']
                                   #}

# COMMAND ----------

# DBTITLE 1,define logging functions
def merge_dicts(dict1, dict2):
    
    if dict1 is None:
        return dict2
    
    if dict2 is None:
        return dict1
    
    merged_dict = dict1.copy()
    merged_dict.update(dict2)
    
    return merged_dict

# trace INFO
def log_trace_info(message, custom_properties_dict=None):
    properties = {'custom_dimensions': merge_dicts(databricks_properties, custom_properties_dict)}
    logger.info(message, extra=properties)

# trace WARNING
def log_trace_warning(message, custom_properties_dict=None):
    properties = {'custom_dimensions': merge_dicts(databricks_properties, custom_properties_dict)}
    logger.warning(message, extra=properties)

# trace ERROR
def log_trace_error(message, custom_properties_dict=None):
    properties = {'custom_dimensions': merge_dicts(databricks_properties, custom_properties_dict)}
    logger.error(message, extra=properties)
    
# log exception ERROR
def log_exception(message, custom_properties_dict=None):
    properties = {'custom_dimensions': merge_dicts(databricks_properties, custom_properties_dict)}    
    logger.exception(message, extra=properties)

# COMMAND ----------

# DBTITLE 1,examples
# HOW TO TRACE
# traces can be seen in app insights by issuing "traces" in the kusto query editor

# info (severity = 1)
log_trace_info("This is an INFO message WITHOUT custom_properties.")
log_trace_info("This is an INFO message WITH custom_properties.", {'key_1': 10, 'key_2': 'value_2'})

# warning (severity = 2)
log_trace_warning("This is a WARNING message WITHOUT custom_properties.")
log_trace_warning("This is a WARNING message WITH custom_properties.", {'key_1': 10, 'key_2': 'value_2'})

# error (severity = 3)
log_trace_error("This is an ERROR message WITHOUT custom_properties.")
log_trace_error("This is an ERROR message WITH custom_properties.", {'key_1': 10, 'key_2': 'value_2'})

# HOW TO TRACE
# exceptions can be seen in app insights by issuing "exceptions" in the kusto query editor
log_exception("This is an EXCEPTION error message.")
log_exception("This is an EXCEPTION error message WITH custom_properties.", {'key_1': 10, 'key_2': 'value_2'})
