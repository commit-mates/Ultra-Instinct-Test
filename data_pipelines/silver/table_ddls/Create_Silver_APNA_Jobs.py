# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC ## Create_Silver_apna_Jobs
# MAGIC
# MAGIC | Date         | Modified By      | Change Log             |
# MAGIC |--------------|------------------|------------------------|
# MAGIC | November 6th 2026| Yateesh Chandra  | Creating the Table : silver.apna_jobs |

# COMMAND ----------

spark.sql("""
        CREATE TABLE IF NOT EXISTS ultra_jobsintel.silver.apna_jobs (
            JOB_ID BIGINT COMMENT 'Job ID from APNA',
            JOB_TITLE STRING COMMENT 'Title of the Job',
            DESCRIPTION STRING COMMENT 'Description of the Job',
            JOB_TYPE STRING COMMENT 'Type Of the Job',
            CATEGORY_ID BIGINT COMMENT 'Category ID of the Job',
            JOB_CATEGORY STRING COMMENT 'Category of the Job',
            DEPARTMENT_ID BIGINT COMMENT 'Department ID of the Job',
            DEPARTMENT_NAME STRING COMMENT 'Department Name of the Job',
            ORGANIZATION_ID BIGINT COMMENT 'Organization ID of the Job',
            ORGANIZATION_NAME STRING COMMENT 'Organization Name of the Job',
            ADDR_LINE_1 STRING COMMENT 'Address Line 1 of the Job',
            ADDR_CITY_ID BIGINT COMMENT 'City ID of the Job',
            ADDR_CITY_NAME STRING COMMENT 'City Name of the Job',
            ADDR_STATE STRING COMMENT 'State of the Job',
            ADDR_LATITUDE FLOAT COMMENT 'Latitude of the Job',
            ADDR_LONGITUDE FLOAT COMMENT 'Longitude of the Job',
            PINCODE BIGINT COMMENT 'Pincode of the Job',
            SHIFT_TYPE STRING COMMENT 'Shift Type of the Job',
            CREATE_DT DATE COMMENT 'Creation Date of the Job',
            EXPIRE_DT DATE COMMENT 'Expiry Date of the Job',
            NUM_OF_OPENINGS BIGINT COMMENT 'Number of Openings of the Job',
            LAST_UPDATED_AT TIMESTAMP COMMENT 'Last Updated At of the Job',
            MIN_EXPERIENCE BIGINT COMMENT 'Minimum Experience of the Job',
            MAX_EXPERIENCE BIGINT COMMENT 'Maximum Experience of the Job',
            MIN_SALARY BIGINT COMMENT 'Minimum Salary of the Job',
            MAX_SALARY BIGINT COMMENT 'Maximum Salary of the Job',
            IS_WFH BOOLEAN COMMENT 'Is WFH of the Job - True / False',
            IS_PART_TIME BOOLEAN COMMENT 'Is Part Time of the Job - True / False',
            BD_CREATE_DT_TM TIMESTAMP COMMENT 'Date and Time of the Job Creation',
            BD_UPDATE_DT_TM TIMESTAMP COMMENT 'Date and Time of the Job Update'
        )
        USING DELTA
    """)
            
