# Databricks notebook source
environment_name = dbutils.secrets.get(scope = "AzureKeyVaultScope", key = "environmentname")
tenant_id = dbutils.secrets.get(scope = "AzureKeyVaultScope", key = "tenantid")
service_principal_id = dbutils.secrets.get(scope = "AzureKeyVaultScope", key = "DatabricksClientId")
service_principal_secret = dbutils.secrets.get(scope = "AzureKeyVaultScope", key = "DatabricksClientSecret")
database_name = "szcuksjac" + environment_name + "sqldb"
#user = dbutils.secrets.get(scope = "AzureKeyVaultScope", key = "SqlAdmin")
#password = dbutils.secrets.get(scope = "AzureKeyVaultScope", key = "SqlPassword")

url = "jdbc:postgresql://szcuksjac" + environment_name + "sql.database.windows.net;databaseName=" + database_name
encrypt = "true"
host_name_in_certificate = "*.database.windows.net"
authority = "https://login.windows.net/" + tenant_id
resource_app_id_url = "https://database.windows.net"

# COMMAND ----------

# waking up the serverless SQL if needed
from time import sleep
import adal

max_minutes_wakeup = 5
interval_check_seconds = 5
loop_range = int(max_minutes_wakeup * 60 / interval_check_seconds)

for i in range(loop_range):
    try:
        context = adal.AuthenticationContext(authority)
        token = context.acquire_token_with_client_credentials(resource_app_id_url, service_principal_id, service_principal_secret)
        
        access_token = token["accessToken"]
        properties = spark._sc._gateway.jvm.java.util.Properties()
        properties.setProperty("accessToken", access_token)

        sql_driver_manager = spark._sc._gateway.jvm.java.sql.DriverManager
        sql_connection = sql_driver_manager.getConnection(url, properties)
        
        df = spark.read \
             .format("jdbc") \
             .option("url", url) \
             .option("dbtable", "sys.all_objects") \
             .option("databaseName", database_name) \
             .option("accessToken", access_token) \
             .option("encrypt", encrypt) \
             .option("hostNameInCertificate", host_name_in_certificate) \
             .load()
        break
    except Exception:
        sleep(interval_check_seconds)

# COMMAND ----------

jdbcDF 
