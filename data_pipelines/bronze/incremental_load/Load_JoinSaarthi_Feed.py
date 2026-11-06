# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC ## Load_JoinSaarthi_Feed
# MAGIC
# MAGIC | Metadata | Detail |
# MAGIC |-----------------------|----------------------|
# MAGIC | **Created By**        | Yateesh Chandra      |
# MAGIC | **Business Logic By** | Yateesh Chandra      |
# MAGIC | **Load Strategy**     | Append               |
# MAGIC | **Source**            | JoinSaarthi API             |
# MAGIC | **Target**            | jobsintel.bronze.raw_joinsaarthi_jobs |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### History
# MAGIC
# MAGIC | Date         | Modified By      | Change Log             |
# MAGIC |--------------|------------------|------------------------|
# MAGIC | November 6th 2026| Yateesh Chandra  |Created Initial Version|

# COMMAND ----------

# DBTITLE 1,Import Libraries
import requests
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Initialize Variables
# Define the URL
URL = "https://joinsaarthi.com/api/jobs"

headers = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    )
}

# Define the Empty List
payload_list = []

# COMMAND ----------

# DBTITLE 1,Define a function to Fetch response from JoinSaarthi Jobs
def fetch_JoinSaarthi_jobs(url, num = 1):
    response = requests.get(url + f"?limit=10", headers=headers, timeout = 90)

    if response.status_code != 200:
        raise Exception(f"API Error : {response.status_code}")

    jobs_list = response.json()
    for payload in jobs_list:
        payload_list.append(json.dumps(payload))
    return payload_list

# COMMAND ----------

# DBTITLE 1,Call the Function
# As per the requirement, we are targetting to capture around 125 jobs
for num in range(1, 3):
    val = fetch_JoinSaarthi_jobs(URL, num)

# COMMAND ----------

# DBTITLE 1,Reading the data into Dataframe
joinsaarthi_df = spark.createDataFrame(val,schema = ['PAYLOAD']) \
            .withColumn("BD_CREATE_DT_TM", current_timestamp()) \
            .withColumn("BD_UPDATE_DT_TM", current_timestamp())

# COMMAND ----------

# DBTITLE 1,Appending the data into Target Table
joinsaarthi_df.write.mode("append").saveAsTable("ultra_jobsintel.bronze.raw_joinsaarthi_jobs")
