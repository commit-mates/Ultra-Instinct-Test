# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ## Create_Schema_Bronze
# MAGIC
# MAGIC  | Date         | Modified By      | Change Log             |
# MAGIC  |--------------|------------------|------------------------|
# MAGIC  | May 30 2026| Yateesh Chandra | Creating the Schema : bronze |

# COMMAND ----------

spark.sql("""USE CATALOG jobsintel""")

# COMMAND ----------

spark.sql("""CREATE SCHEMA IF NOT EXISTS jobsintel.bronze""")
