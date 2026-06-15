# Databricks notebook source
# MAGIC %md
# MAGIC ## Create_Bronze_Raw_Unstop_Jobs
# MAGIC
# MAGIC | Date         | Modified By      | Change Log             |
# MAGIC |--------------|------------------|------------------------|
# MAGIC | June 12th 2026| Yateesh Chandra  | Creating the Table : raw_unstop_jobs |

# COMMAND ----------

spark.sql("""
        CREATE TABLE IF NOT EXISTS jobsintel.bronze.raw_unstop_jobs (
            MESSAGE_ID BIGINT GENERATED ALWAYS AS IDENTITY COMMENT "The identity of the message entering the table sourcing from Unstop",
            PAYLOAD STRING  COMMENT "The Json format of the data called from the website API",
            BD_CREATE_DT_TM TIMESTAMP COMMENT "timestamp loaded",
            BD_UPDATE_DT_TM TIMESTAMP COMMENT "timestamp updated"
        )
        USING DELTA
 """)
