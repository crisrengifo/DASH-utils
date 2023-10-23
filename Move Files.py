# Databricks notebook source
# MAGIC %md
# MAGIC ## Move files from temporary filestore to my mnt area

# COMMAND ----------

# DBTITLE 0,Move files from temporary filestore to my mnt area
tmp_area = 'dbfs:/FileStore/cris_temp'
dbutils.fs.cp(tmp_area, 'dbfs:/mnt/lab/unrestricted/cristobal.rengifo-castillo@defra.gov.uk/hmrc-api/', recurse = True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Once above is complete use below to empty temp area

# COMMAND ----------

dbutils.fs.rm(tmp_area, recurse = True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download file from dbfs to local computer

# COMMAND ----------

# Python function to download file
def download_link(filepath):
    # NB filepath must be in the format dbfs:/ not /dbfs/
    # Get filename
    filename = filepath[filepath.rfind("/"):]
    # Move file to FileStore
    dbutils.fs.cp(filepath, f"dbfs:/FileStore/{filename}")
    # Construct download url
    url = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}/files/{filename}?o={spark.conf.get('spark.databricks.clusterUsageTags.orgId')}"
    # Return html snippet
    return f"<a href={url} target='_blank'>Download file: {filename}</a>"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Other Databricks Utils

# COMMAND ----------

# DBTITLE 1,This will copy a file from one folder to another
dbutils.fs.cp('dbfs:/FileStore/1x_rstudio.sh', 'dbfs:/databricks/scripts/1x_rstudio.sh')

# COMMAND ----------

# DBTITLE 1,This will copy a folders contents to another folder
dbutils.fs.cp(
    'dbfs:/mnt/lab/unrestricted/cristobal.rengifo-castillo@defra.gov.uk/hmrc-api/data', 
    'dbfs:/mnt/lab/unrestricted/cristobal.rengifo-castillo@defra.gov.uk/hmrc-api/data_raw', 
    True
    )

# COMMAND ----------

# DBTITLE 1,This will move a file from one folder to another
dbutils.fs.mv(
    'dbfs:/mnt/lab/unrestricted/cristobal.rengifo-castillo@defra.gov.uk/hmrc-api/CN_CODES_MASTER_TABLE.csv', 
    'dbfs:/mnt/lab/unrestricted/cristobal.rengifo-castillo@defra.gov.uk/hmrc-api/lookup_tables'
    )

# COMMAND ----------

# DBTITLE 1,This will move a folders contents to another folder
dbutils.fs.mv('<pathname>', '<pathdestination>', True)

# COMMAND ----------

# DBTITLE 1,Create a new folder
dbutils.fs.mkdirs('dbfs:/mnt/lab/unrestricted/cristobal.rengifo-castillo@defra.gov.uk/hmrc-api/data_clean')

# COMMAND ----------

# DBTITLE 1,Delete a file/folder
dbutils.fs.rm('dbfs:/mnt/lab/unrestricted/cristobal.rengifo-castillo@defra.gov.uk/hmrc-api/data', True)
