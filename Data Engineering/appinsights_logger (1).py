# Databricks notebook source
from applicationinsights import TelemetryClient, channel
from applicationinsights.logging import enable, LoggingHandler
from applicationinsights.exceptions import enable
import json
import datetime
import time

""" Requirements: requires application insights instrumentationkey (appinsinstrumentationkey) secret to be set within notebook
to enable functions """

app_insight_instrumentation_key = dbutils.secrets.get(scope = "AzureKeyVaultScope", key = "AppInsightsInstrumentationKey")
note_context = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())

def edp_app_insights_tc():
    # set up telemetry channel with custom context properties
    # this helps in filtering logs in azure monitor
    
    tc = TelemetryClient(app_insight_instrumentation_key)
    enable(app_insight_instrumentation_key)
    
    tc.context.properties['role'] = "Databricks"
    tc.context.properties['role_instance'] = spark.conf.get("spark.databricks.workspaceUrl").split('.')[0]
    tc.context.properties['cluster_id'] = note_context['tags']['clusterId']
    tc.context.properties['user'] = note_context['tags']['user']
    tc.context.properties['spark_script_name'] = note_context['extraContext']['notebook_path']
    tc.context.properties['application_id'] = sc.applicationId
    tc.context.properties['spark_version'] = spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion")
    
    return(tc)

# COMMAND ----------

import ast

# custom_properties is a python dictionary : {'key_1': value_1, 'key_2': value_2}
def edp_app_insights_trace(trace_message, custom_properties=None):
    tc = edp_app_insights_tc()
    if (custom_properties is not None):
        for key in custom_properties:
            tc.context.properties[key] = custom_properties[key]   
    tc.track_trace("{0}".format(trace_message))
    tc.flush()

# COMMAND ----------

# custom_properties is a python dictionary : {'key_1': value_1, 'key_2': value_2}
def edp_app_insights_exception(error_message, custom_properties=None):
    tc = edp_app_insights_tc()
    if (custom_properties is not None):
        for key in custom_properties:
            tc.context.properties[key] = custom_properties[key]  
    tc.track_exception("{0}".format(error_message))
    tc.flush()

# COMMAND ----------

def edp_app_insights_metric(metric_message, value):
    tc = edp_app_insights_tc()
    tc.track_metric("{0}".format(metric_message), value)
    tc.flush()

# COMMAND ----------

# HOW TO use trace:
# edp_app_insights_trace("test trace final")
# edp_app_insights_trace("test trace final", "{'prop1': '50', 'prop2': 'aaa'}")

# COMMAND ----------

# HOW TO use exception:
# edp_app_insights_exception("test exception final", "{'prop1': '50', 'prop2': 'aaa'}")

# COMMAND ----------

# HOW TO use metric:
# edp_app_insights_metric("text metric final", 100)

# COMMAND ----------


