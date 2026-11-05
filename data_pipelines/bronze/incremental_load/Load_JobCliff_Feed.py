# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ## Load_JobCliff_Feed
# MAGIC
# MAGIC | Metadata | Detail |
# MAGIC |-----------------------|----------------------|
# MAGIC | **Created By**        | Yateesh Chandra      |
# MAGIC | **Business Logic By** | Yateesh Chandra      |
# MAGIC | **Load Strategy**     | Append               |
# MAGIC | **Source**            | JobCliff API             |
# MAGIC | **Target**            | jobsintel.bronze.raw_jobcliff_jobs |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### History
# MAGIC
# MAGIC | Date         | Modified By      | Change Log             |
# MAGIC |--------------|------------------|------------------------|
# MAGIC | June 15th 2026| Yateesh Chandra  | Created Initial Version|
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
URL = "https://api.jobcliff.com/api/employees/jobs/jobs/"

# Define the headers
headers = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    )
}

# Define the Empty List
job_list = []
payload_list = []

# COMMAND ----------

# DBTITLE 1,Fetch Job Cliff Jobs through API
def fetch_jobcliff_jobs(url):
    response = requests.get(url + "listing?page=1&limit=60&profile=it",
                            headers = headers,
                            timeout = 90)

    if response.status_code != 200:
        raise Exception(f"API Error : {response.status_code}")

    jobs_list = response.json()["data"]

    for row in jobs_list:
        job_list.append(row["id"])

    return job_list

# COMMAND ----------

# DBTITLE 1,Fetch the detailed job information
def detailed_job_info(url, job_id_list):
    for job_id in job_id_list:
        response = requests.get(url + str(job_id),
                                headers = headers)
        payload = response.json()['job']
        payload["application_url"] = 'https://www.jobcliff.com/job-detail/' + str(payload['id'])
        payload_list.append(json.dumps(payload))
    return payload_list
    

# COMMAND ----------

# DBTITLE 1,Call the Function
# As per the requirement, we are targetting to capture around 60 jobs
jobs = fetch_jobcliff_jobs(URL)

# COMMAND ----------

val = detailed_job_info(URL, jobs)

# COMMAND ----------

# DBTITLE 1,Reading the data into Dataframe
jobcliff_df = spark.createDataFrame(val,schema = ['PAYLOAD']) \
            .withColumn("BD_CREATE_DT_TM", current_timestamp()) \
            .withColumn("BD_UPDATE_DT_TM", current_timestamp())

# COMMAND ----------

# DBTITLE 1,Appending the data into Target Table
jobcliff_df.write.mode("append").saveAsTable("ultra_jobsintel.bronze.raw_jobcliff_jobs")
