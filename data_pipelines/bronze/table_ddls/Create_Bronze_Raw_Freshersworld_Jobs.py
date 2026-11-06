# Databricks notebook source
# MAGIC %md
# MAGIC ## Create_Bronze_Raw_Freshersworld_Jobs
# MAGIC
# MAGIC | Date         | Modified By      | Change Log             |
# MAGIC |--------------|------------------|------------------------|
# MAGIC | June 9th 2026| Yateesh Chandra  | Creating the Table : raw_freshersworld_jobs |
# MAGIC | November 6th 2026| Yateesh Chandra  | Migrating to ultra_jobsintel catalog |

# COMMAND ----------

spark.sql("""
        CREATE TABLE IF NOT EXISTS ultra_jobsintel.bronze.raw_freshersworld_jobs (
            MESSAGE_ID BIGINT GENERATED ALWAYS AS IDENTITY COMMENT "The identity of the message entering the table sourcing from freshersworld",
            PAYLOAD STRING  COMMENT "The Json format of the data called from the website",
            BD_CREATE_DT_TM TIMESTAMP COMMENT "timestamp loaded",
            BD_UPDATE_DT_TM TIMESTAMP COMMENT "timestamp updated"
        )
        USING DELTA
 """)
