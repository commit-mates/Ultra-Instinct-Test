# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ## Load_APNA_Feed
# MAGIC
# MAGIC | Metadata | Detail |
# MAGIC |-----------------------|----------------------|
# MAGIC | **Created By**        | Yateesh Chandra      |
# MAGIC | **Business Logic By** | Yateesh Chandra      |
# MAGIC | **Load Strategy**     | Append               |
# MAGIC | **Source**            | Apna API             |
# MAGIC | **Target**            | jobsintel.bronze.raw_apna_jobs |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### History
# MAGIC
# MAGIC | Date         | Modified By      | Change Log             |
# MAGIC |--------------|------------------|------------------------|
# MAGIC | May 30th 2026| Yateesh Chandra  | Created Initial Version|
# MAGIC | November 5th 2026| Yateesh Chandra  | Migrated Catalog and store entire raw payload|

# COMMAND ----------

# DBTITLE 1,Import Libraries
import requests
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Initialize Variables
# Define the URL
URL = "https://production.apna.co/user-profile-orchestrator/public/v1/jobs/"

# Define the Empty List
payload_list = []

# COMMAND ----------

# DBTITLE 1,Define a function to Fetch response from APNA Jobs
def fetch_apna_jobs(url, num = 1):
    response = requests.get(url + f"?page={num}&page_size=25", timeout = 90)

    if response.status_code != 200:
        raise Exception(f"API Error : {response.status_code}")

    jobs_list = response.json()["results"]["jobs"]
    for payload in jobs_list:
        payload_list.append(json.dumps(payload))
    return payload_list

# COMMAND ----------

# DBTITLE 1,Call the Function
# As per the requirement, we are targetting to capture around 125 jobs
for num in range(1, 6):
    val = fetch_apna_jobs(URL, num)

# COMMAND ----------

# DBTITLE 1,Reading the data into Dataframe
apna_df = spark.createDataFrame(val,schema = ['PAYLOAD']) \
            .withColumn("BD_CREATE_DT_TM", current_timestamp()) \
            .withColumn("BD_UPDATE_DT_TM", current_timestamp())

# COMMAND ----------

# DBTITLE 1,Appending the data into Target Table
apna_df.write.mode("append").saveAsTable("ultra_jobsintel.bronze.raw_apna_jobs")
