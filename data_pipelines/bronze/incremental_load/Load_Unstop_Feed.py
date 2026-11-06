# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ## Load_Unstop_Feed
# MAGIC
# MAGIC | Metadata | Detail |
# MAGIC |-----------------------|----------------------|
# MAGIC | **Created By**        | Yateesh Chandra      |
# MAGIC | **Business Logic By** | Yateesh Chandra      |
# MAGIC | **Load Strategy**     | Append               |
# MAGIC | **Source**            | Unstop API             |
# MAGIC | **Target**            | jobsintel.bronze.raw_unstop_jobs |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### History
# MAGIC
# MAGIC | Date         | Modified By      | Change Log             |
# MAGIC |--------------|------------------|------------------------|
# MAGIC | June 12th 2026| Yateesh Chandra  | Created Initial Version|
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
URL = "https://unstop.com/api/public/opportunity/search-result?opportunity=jobs&per_page=20&roles=software-development%2Cfull-stack-development&usertype=fresher"

# Define the headers
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

# DBTITLE 0,Define a function to Fetch response from APNA Jobs
def fetch_unstop_jobs(url, num):
    response = requests.get(url + f"&page={num}", headers = headers, timeout = 90)

    if response.status_code != 200:
        raise Exception(f"API Error : {response.status_code}")

    jobs_list = response.json()["data"]["data"]

    for payload in jobs_list:
        payload_list.append(json.dumps(payload))
    return payload_list

# COMMAND ----------

# DBTITLE 1,Call the Function
# As per the requirement, we are targetting to capture around 60 jobs
for num in range(1, 4):
    val = fetch_unstop_jobs(URL, num)

# COMMAND ----------

# DBTITLE 1,Reading the data into Dataframe
unstop_df = spark.createDataFrame(val,schema = ['PAYLOAD']) \
            .withColumn("BD_CREATE_DT_TM", current_timestamp()) \
            .withColumn("BD_UPDATE_DT_TM", current_timestamp())

# COMMAND ----------

# DBTITLE 1,Appending the data into Target Table
unstop_df.write.mode("append").saveAsTable("ultra_jobsintel.bronze.raw_unstop_jobs")
