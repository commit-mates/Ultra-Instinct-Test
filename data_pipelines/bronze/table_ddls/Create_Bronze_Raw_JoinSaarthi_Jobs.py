# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC ## Create_Bronze_Raw_joinsaarthi_Jobs
# MAGIC
# MAGIC | Date         | Modified By      | Change Log             |
# MAGIC |--------------|------------------|------------------------|
# MAGIC | November 6th 2026| Yateesh Chandra  | Creating the Table : raw_joinsaarthi_jobs |

# COMMAND ----------

spark.sql("""
        CREATE TABLE IF NOT EXISTS ultra_jobsintel.bronze.raw_joinsaarthi_jobs (
            MESSAGE_ID BIGINT GENERATED ALWAYS AS IDENTITY COMMENT "The identity of the message entering the table sourcing from JoinSaarthi",
            PAYLOAD STRING  COMMENT "The Json format of the data called from the API",
            BD_CREATE_DT_TM TIMESTAMP COMMENT "timestamp loaded",
            BD_UPDATE_DT_TM TIMESTAMP COMMENT "timestamp updated"
        )
        USING DELTA
 """)

# COMMAND ----------


