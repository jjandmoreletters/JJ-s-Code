# Databricks notebook source
data_domain = "domain-name-here"
tenant_id = dbutils.secrets.get(scope="AzureKeyVaultScope", key="tenantid")
environment_name = dbutils.secrets.get(scope="AzureKeyVaultScope", key="environment")
service_principal_id = dbutils.secrets.get(scope="AzureKeyVaultScope", key=f"{data_domain.upper()}-UKS-{environment_name.upper()}-DATAPLAT-DATABRICKS-SP-ID")
service_principal_secret = dbutils.secrets.get(scope="AzureKeyVaultScope", key=f"{data_domain.upper()}-UKS-{environment_name.upper()}-DATAPLAT-DATABRICKS-SP-SECRET")
database_name = data_domain + "uks-here" + environment_name + "ssdqjdssad-here"
url = "jdbc:sqlserver://" + data_domain + "uks-here" + environment_name + "ssdpsqlserv.database.windows.net;databaseName=" + database_name
encrypt = "true"
host_name_in_certificate = "*.database.windows.net"
authority = "https://login.windows.net/" + tenant_id
resource_app_id_url = "https://database.windows.net"

# COMMAND ----------

# waking up the serverless SQL if needed
from time import sleep
import adal

max_minutes_wakeup = 2
interval_check_seconds = 10
loop_range = int(max_minutes_wakeup * 60 / interval_check_seconds)

for i in range(loop_range):
    try:
        print(f"Attempt [{i+1} of {loop_range}] : Connecting to {url}")
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
    except Exception as e:
        print(f"Can't connect to {url} after [{i+1} of {loop_range}] attempts : {e}")
        if (i==loop_range-1):
            raise(e)
        sleep(interval_check_seconds)
print(f"Successfully connected to {url} in {i+1} attempts!")
