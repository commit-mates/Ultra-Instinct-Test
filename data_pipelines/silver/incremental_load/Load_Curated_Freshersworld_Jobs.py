# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC ## Load_Freshersworld_Feed
# MAGIC
# MAGIC | Metadata | Detail |
# MAGIC |-----------------------|----------------------|
# MAGIC | **Created By**        | Yateesh Chandra      |
# MAGIC | **Business Logic By** | Yateesh Chandra      |
# MAGIC | **Load Strategy**     | Upsert               |
# MAGIC | **Source**            | ultra_jobsintel.bronze.raw_freshersworld_jobs            |
# MAGIC | **Target**            | ultra_jobsintel.silver.freshersworld_jobs |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### History
# MAGIC
# MAGIC | Date         | Modified By      | Change Log             |
# MAGIC |--------------|------------------|------------------------|
# MAGIC | November 8th 2026| Yateesh Chandra  | Created Initial Version|
# MAGIC

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Provide the Job Location Schema
job_location_schema = ArrayType(
                        StructType([
                            StructField("address",
                                StructType([
                                    StructField("addressCountry", StringType(), True),
                                    StructField("addressLocality", StringType(), True),
                                    StructField("addressRegion", StringType(), True),
                                    StructField("postalCode", StringType(), True),
                                    StructField("streetAddress", StringType(), True)
                                ]), True)
                        ]))

# COMMAND ----------

# DBTITLE 1,Initialize Variables and Dataframes
target_table = "ultra_jobsintel.silver.freshersworld_jobs"
source_freshersworld_df = spark.table("ultra_jobsintel.bronze.raw_freshersworld_jobs")
target_freshersworld_df = spark.table("ultra_jobsintel.silver.freshersworld_jobs")

# COMMAND ----------

# DBTITLE 1,Incremental Load Filter
max_load_dt = target_freshersworld_df.select(max(col('BD_UPDATE_DT_TM'))).collect()[0][0]
print(max_load_dt)

# COMMAND ----------

# DBTITLE 1,Transform the raw Data
freshersworld_curated_df = source_freshersworld_df \
                            .filter(col("BD_UPDATE_DT_TM") > coalesce(lit(max_load_dt), lit("2001-01-01 00:00:00").cast("timestamp"))) \
                            .withColumn("JOB_LOCATION",
                                        from_json(get_json_object(col("PAYLOAD"), "$.jobLocation"),job_location_schema)
                            ) \
                            .select(
                                get_json_object(col('PAYLOAD'), '$.jobId').cast("long").alias("JOB_ID"),
                                get_json_object(col('PAYLOAD'), '$.title').alias("JOB_TITLE"),
                                get_json_object(col('PAYLOAD'), '$.description').alias("DESCRIPTION"),
                                get_json_object(col('PAYLOAD'), '$.hiringOrganization.name').alias("ORGANIZATION_NAME"),
                                coalesce(get_json_object(col('PAYLOAD'), '$.jobLocationType'), lit('OFFICE')).alias("JOB_LOCATION_TYPE"),
                                col("JOB_LOCATION"),
                                get_json_object(col('PAYLOAD'), '$.qualifications').alias("QUALIFICATIONS"),
                                get_json_object(col('PAYLOAD'), '$.datePosted').cast("timestamp").alias("POSTED_AT"),
                                get_json_object(col('PAYLOAD'), '$.validThrough').cast("timestamp").alias("VALID_TILL_DT"),
                                get_json_object(col('PAYLOAD'), '$.employmentType').alias("EMPLOYMENT_TYPE"),
                                get_json_object(col('PAYLOAD'), '$.experienceRequirements.monthsOfExperience').cast("int").alias("EXPERIENCE_IN_MONTHS"),
                                get_json_object(col('PAYLOAD'), '$.baseSalary.value.minValue').cast("long").alias("MIN_SALARY"),
                                get_json_object(col('PAYLOAD'), '$.baseSalary.value.maxValue').cast("long").alias("MAX_SALARY"),
                                get_json_object(col('PAYLOAD'), '$.baseSalary.value.unitText').alias("SALARY_UNIT"),
                                get_json_object(col('PAYLOAD'), '$.jobUrl').alias("JOB_URL"),
                            ) \
                            .withColumn("BD_CREATE_DT_TM", current_timestamp()) \
                            .withColumn("BD_UPDATE_DT_TM", current_timestamp()) 

# COMMAND ----------

# DBTITLE 1,Merge into Target Silver Table
freshersworld_curated_df \
    .mergeInto(target_table, freshersworld_curated_df.JOB_ID == col(f"{target_table}.JOB_ID")) \
    .whenMatched(
        (freshersworld_curated_df.DESCRIPTION != col(f"{target_table}.DESCRIPTION"))
        | (freshersworld_curated_df.JOB_TITLE != col(f"{target_table}.JOB_TITLE"))
        | (freshersworld_curated_df.JOB_LOCATION_TYPE != col(f"{target_table}.JOB_LOCATION_TYPE"))
        | (freshersworld_curated_df.JOB_LOCATION != col(f"{target_table}.JOB_LOCATION"))
        | (freshersworld_curated_df.QUALIFICATIONS != col(f"{target_table}.QUALIFICATIONS"))
        | (freshersworld_curated_df.POSTED_AT != col(f"{target_table}.POSTED_AT"))
        | (freshersworld_curated_df.VALID_TILL_DT != col(f"{target_table}.VALID_TILL_DT"))
        | (freshersworld_curated_df.EMPLOYMENT_TYPE != col(f"{target_table}.EMPLOYMENT_TYPE"))
        | (freshersworld_curated_df.EXPERIENCE_IN_MONTHS != col(f"{target_table}.EXPERIENCE_IN_MONTHS"))
        | (freshersworld_curated_df.MIN_SALARY != col(f"{target_table}.MIN_SALARY"))
        | (freshersworld_curated_df.MAX_SALARY != col(f"{target_table}.MAX_SALARY"))
        | (freshersworld_curated_df.SALARY_UNIT != col(f"{target_table}.SALARY_UNIT"))
    ).update(
        {
            'JOB_TITLE' : freshersworld_curated_df.JOB_TITLE,
            'DESCRIPTION' : freshersworld_curated_df.DESCRIPTION,
            'JOB_LOCATION_TYPE' : freshersworld_curated_df.JOB_LOCATION_TYPE,
            'JOB_LOCATION' : freshersworld_curated_df.JOB_LOCATION,
            'QUALIFICATIONS' : freshersworld_curated_df.QUALIFICATIONS,
            'POSTED_AT' : freshersworld_curated_df.POSTED_AT,
            'VALID_TILL_DT' : freshersworld_curated_df.VALID_TILL_DT,
            'EMPLOYMENT_TYPE' : freshersworld_curated_df.EMPLOYMENT_TYPE,
            'EXPERIENCE_IN_MONTHS' : freshersworld_curated_df.EXPERIENCE_IN_MONTHS,
            'MIN_SALARY' : freshersworld_curated_df.MIN_SALARY,
            'MAX_SALARY' : freshersworld_curated_df.MAX_SALARY,
            'SALARY_UNIT' : freshersworld_curated_df.SALARY_UNIT,
            'BD_UPDATE_DT_TM' : freshersworld_curated_df.BD_UPDATE_DT_TM
        }
    ) \
    .whenNotMatched().insertAll() \
    .merge()
