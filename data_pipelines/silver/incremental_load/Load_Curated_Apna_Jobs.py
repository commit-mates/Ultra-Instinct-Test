# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC ## Load_APNA_Feed
# MAGIC
# MAGIC | Metadata | Detail |
# MAGIC |-----------------------|----------------------|
# MAGIC | **Created By**        | Yateesh Chandra      |
# MAGIC | **Business Logic By** | Yateesh Chandra      |
# MAGIC | **Load Strategy**     | Upsert               |
# MAGIC | **Source**            | ultra_jobsintel.bronze.raw_apna_jobs            |
# MAGIC | **Target**            | ultra_jobsintel.silver.apna_jobs |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### History
# MAGIC
# MAGIC | Date         | Modified By      | Change Log             |
# MAGIC |--------------|------------------|------------------------|
# MAGIC | November 7th 2026| Yateesh Chandra  | Created Initial Version|
# MAGIC | November 12th 2026| Yateesh Chandra  | Included Incremental Load Filter|
# MAGIC

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Initialize Variables and Dataframes
target_table = "ultra_jobsintel.silver.apna_jobs"
source_apna_df = spark.table("ultra_jobsintel.bronze.raw_apna_jobs")
target_apna_df = spark.table("ultra_jobsintel.silver.apna_jobs")
max_load_dt = target_apna_df.select(max(col('BD_UPDATE_DT_TM'))).collect()[0][0]

# COMMAND ----------

print(max_load_dt)

# COMMAND ----------

# DBTITLE 1,Transform the raw Data
apna_curated_df = source_apna_df \
                    .filter(col("BD_CREATE_DT_TM") > coalesce(lit(max_load_dt), lit("2001-01-01 00:00:00").cast("timestamp"))) \
                    .select(
                        get_json_object(col('PAYLOAD'), '$.id').cast("long").alias("JOB_ID"),
                        get_json_object(col('PAYLOAD'), '$.title').alias("JOB_TITLE"),
                        get_json_object(col('PAYLOAD'), '$.description').alias("DESCRIPTION"),
                        get_json_object(col('PAYLOAD'), '$.type').alias("JOB_TYPE"),
                        get_json_object(col('PAYLOAD'), '$.category_id').cast("long").alias("CATEGORY_ID"),
                        get_json_object(col('PAYLOAD'), '$.category').alias("JOB_CATEGORY"),
                        get_json_object(col('PAYLOAD'), '$.department.id').cast("long").alias("DEPARTMENT_ID"),
                        get_json_object(col('PAYLOAD'), '$.department.name').alias("DEPARTMENT_NAME"),
                        get_json_object(col('PAYLOAD'), '$.organization.id').cast("long").alias("ORGANIZATION_ID"),
                        get_json_object(col('PAYLOAD'), '$.organization.name').alias("ORGANIZATION_NAME"),
                        get_json_object(col('PAYLOAD'), '$.address.line_1').alias("ADDR_LINE_1"),
                        get_json_object(col('PAYLOAD'), '$.address.city.id').cast("long").alias("ADDR_CITY_ID"),
                        get_json_object(col('PAYLOAD'), '$.address.city.name').alias("ADDR_CITY_NAME"),
                        get_json_object(col('PAYLOAD'), '$.address.city.json_data.state').alias("ADDR_STATE"),
                        get_json_object(col('PAYLOAD'), '$.address.city.mid_area.latitude').cast("float").alias("ADDR_LATITUDE"),
                        get_json_object(col('PAYLOAD'), '$.address.city.mid_area.longitude').cast("float").alias("ADDR_LONGITUDE"),
                        coalesce(
                            get_json_object(col('PAYLOAD'), '$.address.city.mid_area.json_data.pincode').try_cast("long"),
                            get_json_object(col('PAYLOAD'), '$.company_address.area.pincode').try_cast("long")
                        ).alias("PINCODE"),
                        get_json_object(col('PAYLOAD'), '$.shift').alias("SHIFT_TYPE"),
                        get_json_object(col('PAYLOAD'), '$.created_on').cast("date").alias("CREATE_DT"),
                        get_json_object(col('PAYLOAD'), '$.expiry').cast("date").alias("EXPIRE_DT"),
                        get_json_object(col('PAYLOAD'), '$.no_of_openings').cast("long").alias("NUM_OF_OPENINGS"),
                        get_json_object(col('PAYLOAD'), '$.last_updated').cast("timestamp").alias("LAST_UPDATED_AT"),
                        get_json_object(col('PAYLOAD'), '$.min_experience').cast("long").alias("MIN_EXPERIENCE"),
                        get_json_object(col('PAYLOAD'), '$.max_experience').cast("long").alias("MAX_EXPERIENCE"),
                        get_json_object(col('PAYLOAD'), '$.min_salary').cast("long").alias("MIN_SALARY"),
                        get_json_object(col('PAYLOAD'), '$.max_salary').cast("long").alias("MAX_SALARY"),
                        get_json_object(col('PAYLOAD'), '$.is_wfh').cast("boolean").alias("IS_WFH"),
                        get_json_object(col('PAYLOAD'), '$.is_part_time').cast("boolean").alias("IS_PART_TIME"),
                        col("BD_CREATE_DT_TM").cast("date").alias("last_load_dt")
                    ) \
                    .withColumn("BD_CREATE_DT_TM", current_timestamp()) \
                    .withColumn("BD_UPDATE_DT_TM", current_timestamp()) 

# COMMAND ----------

apna_curated_df \
    .mergeInto(target_table, apna_curated_df.JOB_ID == col(f"{target_table}.JOB_ID")) \
    .whenMatched(
        (apna_curated_df.ADDR_CITY_ID != col(f"{target_table}.ADDR_CITY_ID"))
        | (apna_curated_df.ADDR_STATE != col(f"{target_table}.ADDR_STATE"))
        | (apna_curated_df.ADDR_LATITUDE != col(f"{target_table}.ADDR_LATITUDE"))
        | (apna_curated_df.ADDR_LONGITUDE != col(f"{target_table}.ADDR_LONGITUDE"))
        | (apna_curated_df.SHIFT_TYPE != col(f"{target_table}.SHIFT_TYPE"))
        | (apna_curated_df.EXPIRE_DT != col(f"{target_table}.EXPIRE_DT"))
        | (apna_curated_df.NUM_OF_OPENINGS != col(f"{target_table}.NUM_OF_OPENINGS"))
        | (apna_curated_df.LAST_UPDATED_AT != col(f"{target_table}.LAST_UPDATED_AT"))
        | (apna_curated_df.MIN_EXPERIENCE != col(f"{target_table}.MIN_EXPERIENCE"))
        | (apna_curated_df.MAX_EXPERIENCE != col(f"{target_table}.MAX_EXPERIENCE"))
        | (apna_curated_df.MIN_SALARY != col(f"{target_table}.MIN_SALARY"))
        | (apna_curated_df.MAX_SALARY != col(f"{target_table}.MAX_SALARY"))
        | (apna_curated_df.IS_WFH != col(f"{target_table}.IS_WFH"))
        | (apna_curated_df.IS_PART_TIME != col(f"{target_table}.IS_PART_TIME"))
        | (apna_curated_df.JOB_TYPE != col(f"{target_table}.JOB_TYPE"))
    ).update(
        {
            'JOB_TITLE' : apna_curated_df.JOB_TITLE,
            'DESCRIPTION' : apna_curated_df.DESCRIPTION,
            'JOB_TYPE' : apna_curated_df.JOB_TYPE,
            'CATEGORY_ID' : apna_curated_df.CATEGORY_ID,
            'JOB_CATEGORY' : apna_curated_df.JOB_CATEGORY,
            'DEPARTMENT_ID' : apna_curated_df.DEPARTMENT_ID,
            'DEPARTMENT_NAME' : apna_curated_df.DEPARTMENT_NAME,
            'ORGANIZATION_ID' : apna_curated_df.ORGANIZATION_ID,
            'ORGANIZATION_NAME' : apna_curated_df.ORGANIZATION_NAME,
            'ADDR_LINE_1' : apna_curated_df.ADDR_LINE_1,
            'ADDR_CITY_ID' : apna_curated_df.ADDR_CITY_ID,
            'ADDR_CITY_NAME' : apna_curated_df.ADDR_CITY_NAME,
            'ADDR_STATE' : apna_curated_df.ADDR_STATE,
            'ADDR_LATITUDE' : apna_curated_df.ADDR_LATITUDE,
            'ADDR_LONGITUDE' : apna_curated_df.ADDR_LONGITUDE,
            'PINCODE' : apna_curated_df.PINCODE,
            'SHIFT_TYPE' : apna_curated_df.SHIFT_TYPE,
            'CREATE_DT' : apna_curated_df.CREATE_DT,
            'EXPIRE_DT' : apna_curated_df.EXPIRE_DT,
            'NUM_OF_OPENINGS' : apna_curated_df.NUM_OF_OPENINGS,
            'LAST_UPDATED_AT' : apna_curated_df.LAST_UPDATED_AT,
            'MIN_EXPERIENCE' : apna_curated_df.MIN_EXPERIENCE,
            'MAX_EXPERIENCE' : apna_curated_df.MAX_EXPERIENCE,
            'MIN_SALARY' : apna_curated_df.MIN_SALARY,
            'MAX_SALARY' : apna_curated_df.MAX_SALARY,
            'IS_WFH' : apna_curated_df.IS_WFH,
            'IS_PART_TIME' : apna_curated_df.IS_PART_TIME,
            'BD_UPDATE_DT_TM' : apna_curated_df.BD_UPDATE_DT_TM
        }
    ) \
    .whenNotMatched().insertAll() \
    .merge()
