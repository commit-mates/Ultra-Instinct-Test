# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ## Load_Freshersworld_Feed
# MAGIC
# MAGIC | Metadata | Detail |
# MAGIC |-----------------------|----------------------|
# MAGIC | **Created By**        | Yateesh Chandra      |
# MAGIC | **Business Logic By** | Yateesh Chandra      |
# MAGIC | **Load Strategy**     | Append               |
# MAGIC | **Source**            | Scraping Freshersworld             |
# MAGIC | **Target**            | jobsintel.bronze.raw_freshersworld_jobs |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### History
# MAGIC
# MAGIC | Date         | Modified By      | Change Log             |
# MAGIC |--------------|------------------|------------------------|
# MAGIC | June 9th 2026| Yateesh Chandra  | Created Initial Version|

# COMMAND ----------

# DBTITLE 1,Import Libraries
import requests
import json
from bs4 import BeautifulSoup
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Initialize Variables
# Define the URL
URL = "https://www.freshersworld.com/jobs/category/it-software-job-vacancies"

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

# DBTITLE 1,Define a function to Fetch response from Freshersworld web
def fetch_freshersworld_jobs(url, num):
    response = requests.get(f"{url}?&offset={num}", headers=headers)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    for card in soup.select("div.job-container, div.job-card, div.latest-jobs, div.job-new-title"):  
        title = card.select_one(".job-new-title")
        company = card.select_one(".company-name")
        location = card.select_one("job-location")
        experience = card.select_one(".experience")
        payload = {
            "job_id": card.get("job_id"),
            "job_url" : card.get("job_display_url"),
            "title": title.get_text(strip=True) if title else None,
            "company": company.get_text(strip=True) if company else None,
            "location": location.get_text(strip=True) if location else None,
            "experience": experience.get_text(strip=True) if experience else None,
        }
        payload_list.append(json.dumps(payload))

    return payload_list

# COMMAND ----------

# DBTITLE 1,Used for testing
num = 0
val = [(x,) for x in fetch_freshersworld_jobs(URL, num)]
print(val)

# COMMAND ----------

# DBTITLE 1,Call the Function
for num in range(0, 101, 20):
    val = [(x,) for x in fetch_freshersworld_jobs(URL, num)]

# COMMAND ----------

# DBTITLE 1,Reading the data into Dataframe
freshersworld_df = spark.createDataFrame(val,schema = ['PAYLOAD']) \
            .withColumn("BD_CREATE_DT_TM", current_timestamp()) \
            .withColumn("BD_UPDATE_DT_TM", current_timestamp())

# COMMAND ----------

# DBTITLE 1,Appending the data into Target Table
freshersworld_df.write.mode("append").saveAsTable("jobsintel.bronze.raw_freshersworld_jobs")
