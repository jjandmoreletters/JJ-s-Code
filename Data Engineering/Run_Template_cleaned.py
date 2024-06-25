# Databricks notebook source
# DBTITLE 1,Notebook parameters
# start time for the processing window
dbutils.widgets.text("WindowStartTime","")

# end time for the processing window
dbutils.widgets.text("WindowEndTime","")

# debug flag used for verbose logging
dbutils.widgets.text("Debug","False")

# COMMAND ----------

# DBTITLE 1,Notebook Parameters checks
debug = dbutils.widgets.get("Debug")
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

window_start_time = dbutils.widgets.get("WindowStartTime")
if window_start_time=="":
      raise Exception("No WindowStartTime provided, please provide a value for WindowStartTime in this format (py syntax): %Y-%m-%dT%H:%M:%SZ")

# COMMAND ----------

# Function to Run Notebook with Retry

def run_notebook_with_retry(Tempfilename):
  notebook = 'Template_Processing_cleaned'
  num_retries = 0
  while  num_retries <= 1:
    try:
      dbutils.notebook.run(notebook, 600, {"TempFileName": Tempfilename,"WindowStartTime": window_start_time})
      print (Tempfilename,' - Success')
      break
    except Exception as excep:
      if num_retries >= 1:
        print(Tempfilename + ' - Failed')
        break
      else:
        num_retries += 1
        print("Retrying - ", Tempfilename)
        


# COMMAND ----------

# Run Notebooks in Parellel batches of 10

from multiprocessing.pool import ThreadPool

pool = ThreadPool(10)

Tempfiles = [
  'Table1'
 ,'Table2'
 ,'Table3'
 ,'Table4'
 ,'Table5'
]


pool.map(lambda Tempfilename: run_notebook_with_retry(Tempfilename),Tempfiles)

# COMMAND ----------


