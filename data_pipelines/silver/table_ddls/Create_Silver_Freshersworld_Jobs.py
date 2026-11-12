# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC ## Create_Silver_Freshersworld_Jobs
# MAGIC
# MAGIC | Date         | Modified By      | Change Log             |
# MAGIC |--------------|------------------|------------------------|
# MAGIC | November 8th 2026| Yateesh Chandra  | Creating the Table : silver.freshersworld_jobs |

# COMMAND ----------

spark.sql("""
    CREATE TABLE ultra_jobsintel.silver.freshersworld_jobs (
        JOB_ID BIGINT COMMENT 'Unique Job ID',
        JOB_TITLE STRING COMMENT 'Job Title',
        DESCRIPTION STRING COMMENT 'Job Description',
        ORGANIZATION_NAME STRING COMMENT 'Organization Name',
        JOB_LOCATION_TYPE STRING COMMENT 'Job Location Type',
        JOB_LOCATION ARRAY<
        STRUCT<
            address: STRUCT<
                addressCountry: STRING,
                addressLocality: STRING,
                addressRegion: STRING, 
                postalCode: STRING,
                streetAddress: STRING
            >
        >
        > COMMENT 'Job Location',
        POSTED_AT TIMESTAMP COMMENT 'Job Post Date Timestamp',
        VALID_TILL_DT TIMESTAMP COMMENT 'Job Valid Through Date Date',
        QUALIFICATIONS STRING COMMENT 'Qualifications required for the Job',
        EMPLOYMENT_TYPE STRING COMMENT 'Employment Type',
        EXPERIENCE_IN_MONTHS INTEGER COMMENT 'Experience in Months',
        MIN_SALARY BIGINT COMMENT 'Minimum Salary',
        MAX_SALARY BIGINT COMMENT 'Maximum Salary',
        SALARY_UNIT STRING COMMENT 'Salary Unit Text - Monthly / Yearly',
        JOB_URL STRING COMMENT 'Job URL',
        BD_CREATE_DT_TM TIMESTAMP COMMENT 'Data Engineering Create Timestamp',
        BD_UPDATE_DT_TM TIMESTAMP COMMENT 'Data Engineering Update Timestamp'
    )
    USING delta
""")
