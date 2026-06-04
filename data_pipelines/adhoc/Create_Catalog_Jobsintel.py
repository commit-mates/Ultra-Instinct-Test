# Databricks notebook source
# MAGIC %md
# MAGIC ## Create_catalog_jobsintel
# MAGIC
# MAGIC  | Date         | Modified By      | Change Log             |
# MAGIC  |--------------|------------------|------------------------|
# MAGIC  | May 30 2026| Yateesh Chandra | Creating the Catalog : jobsintel |

# COMMAND ----------

spark.sql("""CREATE CATALOG IF NOT EXISTS jobsintel""")
