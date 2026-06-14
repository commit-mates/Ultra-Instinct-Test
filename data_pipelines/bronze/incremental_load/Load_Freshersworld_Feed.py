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
job_list = []

# COMMAND ----------

# DBTITLE 1,Define a function to get list of Job URLs
def get_freshersworld_job_urls(url, num):
    response = requests.get(f"{url}?&offset={num}", headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")

    jobs = []
    for card in soup.select(
        "div.job-container, div.job-card, div.latest-jobs, div.job-new-title"
    ):
        job_url = card.get("job_display_url")
        if job_url:
            jobs.append(job_url)

    return jobs

# COMMAND ----------

# DBTITLE 1,Define a function to Fetch response from Freshersworld web
def fetch_freshersworld_jobs(job_list):
    for URL in job_list :
        response = requests.get(URL, headers=headers)
        soup = BeautifulSoup(response.text, "html.parser")

        job_id = URL.split("-")[-1]
        title = soup.select_one(".job-role h2")
        company = soup.select_one(".company-name span")
        job_desc = soup.select_one(".job-desc")
        skills = soup.select_one(".skills-detail")
        location = soup.select_one(".job-location a")
        experience = soup.select_one("div.job-details-sub-div span.space")
        salary = soup.select_one(" div.salary div.job-details-sub-div span.space")

        payload = {
            "job_id": job_id,
            "job_url" : URL,
            "title": title.get_text(strip=True) if title else None,
            "job_desc" : job_desc.get_text(separator="\n", strip=True) if job_desc else None,
            "company": company.get_text(strip=True) if company else None,
            "location": location.get_text(strip=True) if location else None,
            "experience": experience.get_text(strip=True) if experience else None,
            "salary" : salary.get_text(strip=True) if salary else None,
            "skills": skills.get_text(separator=", ", strip=True) if skills else None
        }

        payload_list.append(json.dumps(payload))
    return payload_list

# COMMAND ----------

# DBTITLE 1,Call the Function
for num in range(0, 101, 20):
    job_list.extend(get_freshersworld_job_urls(URL, num))

val = [(x,) for x in fetch_freshersworld_jobs(job_list)]

# COMMAND ----------

# DBTITLE 1,Reading the data into Dataframe
freshersworld_df = spark.createDataFrame(val,schema = ['PAYLOAD']) \
            .withColumn("BD_CREATE_DT_TM", current_timestamp()) \
            .withColumn("BD_UPDATE_DT_TM", current_timestamp())

# COMMAND ----------

# DBTITLE 1,Appending the data into Target Table
# freshersworld_df.write.mode("append").saveAsTable("jobsintel.bronze.raw_freshersworld_jobs")
