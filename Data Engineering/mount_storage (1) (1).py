# Databricks notebook source
# DBTITLE 1,Get tenant and environment name secrets
data_domain = "domain-name-here"
tenant_id = dbutils.secrets.get(scope="AzureKeyVaultScope", key="tenantid")
environment_name = dbutils.secrets.get(scope="AzureKeyVaultScope", key="environment")
service_principal_id = dbutils.secrets.get(scope="AzureKeyVaultScope", key=f"{data_domain.upper()}-UKS-{environment_name.upper()}-DATAPLAT-DATABRICKS-SP-ID")
service_principal_secret = dbutils.secrets.get(scope="AzureKeyVaultScope", key=f"{data_domain.upper()}-UKS-{environment_name.upper()}-DATAPLAT-DATABRICKS-SP-SECRET")

# COMMAND ----------

# best practices for Azure Data Lake Gen2
data_protocol = "abfss://"
data_api = ".dfs.core.windows.net"
data_catalog = f"{data_domain}_{environment_name}"

datalake_storage_account_name = data_domain + "uks-name-here" + environment_name + "ssd-datalake-here"
raw_path = data_protocol + "raw@" + datalake_storage_account_name + data_api
cleaned_path = data_protocol + "cleaned@" + datalake_storage_account_name + data_api
conformed_path = data_protocol + "conformed@" + datalake_storage_account_name + data_api

spark.conf.set("fs.azure.account.auth.type."+datalake_storage_account_name+".dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type."+datalake_storage_account_name+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id."+datalake_storage_account_name+".dfs.core.windows.net", service_principal_id)
spark.conf.set("fs.azure.account.oauth2.client.secret."+datalake_storage_account_name+".dfs.core.windows.net", service_principal_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+datalake_storage_account_name+".dfs.core.windows.net", "https://login.microsoftonline.com/"+tenant_id+"/oauth2/token")

# COMMAND ----------

spark.sql(f"USE CATALOG {data_catalog}")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS cleaned;
# MAGIC CREATE DATABASE IF NOT EXISTS conformed;
