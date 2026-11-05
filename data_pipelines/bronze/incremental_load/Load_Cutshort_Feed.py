# Databricks notebook source
# MAGIC %md
# MAGIC ## Load_Cutshort_Feed
# MAGIC
# MAGIC | Metadata | Detail |
# MAGIC |-----------------------|----------------------|
# MAGIC | **Created By**        | Yateesh Chandra      |
# MAGIC | **Business Logic By** | Yateesh Chandra      |
# MAGIC | **Load Strategy**     | Append               |
# MAGIC | **Source**            | Scraping Cutshort             |
# MAGIC | **Target**            | ultra_jobsintel.bronze.raw_cutshort_jobs |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### History
# MAGIC
# MAGIC | Date         | Modified By      | Change Log             |
# MAGIC |--------------|------------------|------------------------|
# MAGIC | June 18th 2026| Yateesh Chandra  | Created Initial Version|
# MAGIC | November 5th 2026| Yateesh Chandra  | Changed the Data Payload and catalog|

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
URL = "https://cutshort.io/jobs/python-jobs"

headers = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    )
}

# Define the Empty List 
payload_list = []
job_list = []

session = requests.Session()
session.headers.update(headers)

# COMMAND ----------

# DBTITLE 1,Function to extract Embedded JSON from Job Page
def extract_job_data(soup):

    extracted_jobs = []
    for script in soup.find_all("script"):
        script_type = script.get("type", "").lower()
        if script_type not in ("application/json","application/ld+json"):
            continue

        try:
            script_data = json.loads(
                script.string or script.get_text()
            )
            jobs = find_job_objects(script_data)
            extracted_jobs.extend(jobs)
        except (json.JSONDecodeError, TypeError):
            continue

    return extracted_jobs

# COMMAND ----------

# DBTITLE 1,Define a function to fetch jobs url list
def fetch_cutshort_job_list(url):

    response = session.get(url, timeout=15)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or script.get_text()) 
            if data.get("@type") == "ItemList":
                for item in data.get("itemListElement", []):
                    job_url = item.get("url")
                    if job_url:
                        job_list.append(job_url)
        except (json.JSONDecodeError, AttributeError):
            continue
    return list(dict.fromkeys(job_list))


# COMMAND ----------

# DBTITLE 1,Define a function to find Job Objects
def find_job_objects(data):
    jobs = []
    if isinstance(data, dict):
        if (data.get("publicUrl") and data.get("headline")):
            jobs.append(data)
        for value in data.values():
            jobs.extend(find_job_objects(value))

    elif isinstance(data, list):
        for item in data:
            jobs.extend(find_job_objects(item))

    return jobs

# COMMAND ----------

# DBTITLE 1,Fetch the list of values
jobs = fetch_cutshort_job_list(URL)

# COMMAND ----------

# DBTITLE 1,Define a function to get Individual Job details
def get_cutshort_job(jobs):

    for url in jobs:
        if not url:
            continue
        try:
            response = session.get(url, timeout=15)
            response.raise_for_status()

            soup = BeautifulSoup(response.text,"html.parser")
            extracted_jobs = extract_job_data(soup)

            if extracted_jobs:
                for job in extracted_jobs:
                    payload_list.append(json.dumps(job,ensure_ascii=False))
            else:
                print(f"No embedded job JSON found: {url}")

        except requests.RequestException as e:
            print(f"Failed to fetch {url}: {e}")

    return payload_list

# COMMAND ----------

# DBTITLE 1,Call the Function
val = get_cutshort_job(job_list)

# COMMAND ----------

# DBTITLE 1,Reading the data into Dataframe
cutshort_df = spark.createDataFrame(val,schema = ['PAYLOAD']) \
            .withColumn("BD_CREATE_DT_TM", current_timestamp()) \
            .withColumn("BD_UPDATE_DT_TM", current_timestamp())

# COMMAND ----------

# DBTITLE 1,Remove Non Nulls in the dataframe
refined_cutshort_df = cutshort_df.filter("payload:headline is not null")

# COMMAND ----------

# DBTITLE 1,Appending the data into Target Table
refined_cutshort_df.write.mode("append").saveAsTable("ultra_jobsintel.bronze.raw_cutshort_jobs")
