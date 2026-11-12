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
# MAGIC | November 5th 2026| Yateesh Chandra  | Migrated Catalog and store entire raw payload |
# MAGIC | November 12th 2026| Yateesh Chandra  | Introduced Job_Id and Job_Url |

# COMMAND ----------

# DBTITLE 1,Import Libraries
import json
import requests
import threading
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor
from requests.adapters import HTTPAdapter
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Initialize Variables
BASE_URL = "https://www.freshersworld.com/jobs/category/it-software-job-vacancies"
PAGE_OFFSETS = range(0, 101, 20)
TIMEOUT = (10, 30)

CARD_SELECTOR = (
    "div.job-container, "
    "div.job-card, "
    "div.latest-jobs, "
    "div.job-new-title"
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    )
}

thread_local = threading.local()

# COMMAND ----------

# DBTITLE 1,Function to get session
def get_session():

    if not hasattr(thread_local, "session"):
        session = requests.Session()
        adapter = HTTPAdapter(pool_connections=5, pool_maxsize=5)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        session.headers.update(HEADERS)
        thread_local.session = session
    return thread_local.session

# COMMAND ----------

# DBTITLE 1,Define a function to get list of Job URLs
def get_freshersworld_job_urls():
    job_urls = []

    for offset in PAGE_OFFSETS:
        try:
            response = get_session().get(
                BASE_URL,
                params={"offset": offset},
                timeout=TIMEOUT
            )
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            page_urls = [
                urljoin(BASE_URL, card["job_display_url"])
                for card in soup.select(CARD_SELECTOR)
                if card.get("job_display_url")
            ]
            job_urls.extend(page_urls)
            print(
                f"Offset: {offset} | "
                f"Jobs Found: {len(page_urls)}"
            )
        except requests.RequestException as e:
            print(f"Listing page failed | Offset: {offset} | {e}")

    return list(dict.fromkeys(job_urls))

# COMMAND ----------

# DBTITLE 1,Identify Job Posting JSON Objects
def extract_job_postings(data):
    if isinstance(data, list):
        for item in data:
            yield from extract_job_postings(item)
    elif isinstance(data, dict):
        job_type = data.get("@type", [])

        if isinstance(job_type, str):
            job_type = [job_type]
        if any(
            str(t).rstrip("/").split("/")[-1] == "JobPosting"
            for t in job_type
        ):
            yield data
        # Handle nested JSON-LD @graph
        if "@graph" in data:
            yield from extract_job_postings(data["@graph"])

# COMMAND ----------

# DBTITLE 1,Extract Job IDs
def get_job_id(job):

    identifier = job.get("identifier")
    if isinstance(identifier, dict):
        identifier = (identifier.get("value") or identifier.get("name"))

    elif isinstance(identifier, list):
        identifier = next(
            (
                item.get("value")
                for item in identifier
                if isinstance(item, dict) and item.get("value")
            ),
            None
        )

    return str(
        identifier
        or job.get("url")
        or job.get("@id")
        or json.dumps(job, sort_keys=True, ensure_ascii=False)
    )

# COMMAND ----------

# DBTITLE 1,Fetch the data from jobposting_url

def fetch_jobposting_data(job_url):

    try:
        response = get_session().get(job_url, timeout=TIMEOUT)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        clean_url = job_url.split("?")[0].split("#")[0].rstrip("/")
        job_id = clean_url.split("-")[-1]

        job_records = []

        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string or script.get_text())
            except (json.JSONDecodeError, TypeError):
                continue

            for job in extract_job_postings(data):
                job_payload = {
                    "jobId": job_id,
                    "jobUrl": job_url,
                    **job
                }
                job_payload.pop("@type")
                job_payload.pop("@context")
                payload = json.dumps(job_payload, ensure_ascii=False)
                job_records.append((job_id, payload))
        return job_records

    except requests.RequestException as e:
        print(f"Failed URL: {job_url} | Error: {e}")
        return []

# COMMAND ----------

# DBTITLE 1,Call the Function
job_urls = get_freshersworld_job_urls()

seen_job_ids = set()
job_payloads = []

with ThreadPoolExecutor(max_workers=3) as executor:
    for records in executor.map(fetch_jobposting_data, job_urls):
        for job_id, payload in records:
            if job_id not in seen_job_ids:
                seen_job_ids.add(job_id)
                job_payloads.append(payload)

# COMMAND ----------

# DBTITLE 1,Reading the data into Dataframe
freshersworld_df = spark.createDataFrame(
                        [(payload,) for payload in job_payloads],
                        schema = ['PAYLOAD']
                    ) \
                    .withColumn("BD_CREATE_DT_TM", current_timestamp()) \
                    .withColumn("BD_UPDATE_DT_TM", current_timestamp())

# COMMAND ----------

# DBTITLE 1,Appending the data into Target Table
freshersworld_df.write.mode("append").saveAsTable("ultra_jobsintel.bronze.raw_freshersworld_jobs")
