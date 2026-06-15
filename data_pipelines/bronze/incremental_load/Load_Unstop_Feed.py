# Databricks notebook source
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
    response = requests.get(url + f"&page={num}", timeout = 90)

    if response.status_code != 200:
        raise Exception(f"API Error : {response.status_code}")

    jobs_list = response.json()["data"]["data"]

    for row in jobs_list:
        payload = {
            "job_id" : row['id'],
            "title" : row['title'],
            "job_desc" : row['details'],
            "job_type" : row['jobDetail']['timing'],
            "company_id" : row['organization_id'],
            "company_name" : row['organisation']['name'],
            "created_on" : row['regnRequirements']['start_regn_dt'],
            "last_updated" : row['updated_at'],
            "end_date" : row['end_date'],
            "min_experience" : row['jobDetail']['min_experience'],
            "max_experience" : row['jobDetail']['max_experience'],
            "skills" : ', '.join([item["skill"] for item in row["required_skills"]]),
            "application_url" : row['seo_url'],
            "location" : row['jobDetail']['locations']
        }
        payload_list.append(json.dumps(payload))
    return payload_list

# COMMAND ----------

# DBTITLE 1,Call the Function
# As per the requirement, we are targetting to capture around 20 jobs
num = 1
val = fetch_unstop_jobs(URL, num)

# COMMAND ----------

# DBTITLE 1,Reading the data into Dataframe
unstop_df = spark.createDataFrame(val,schema = ['PAYLOAD']) \
            .withColumn("BD_CREATE_DT_TM", current_timestamp()) \
            .withColumn("BD_UPDATE_DT_TM", current_timestamp())

# COMMAND ----------

# DBTITLE 1,Appending the data into Target Table
unstop_df.write.mode("append").saveAsTable("jobsintel.bronze.raw_unstop_jobs")
