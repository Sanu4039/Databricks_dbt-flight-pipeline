# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE VOLUME workspace.raw_schema.rawvolume

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/workspace/raw_schema/rawvolume/raw_data")

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/workspace/raw_schema/rawvolume/raw_data/bookings")

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/workspace/raw_schema/rawvolume/raw_data/flights")

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/workspace/raw_schema/rawvolume/raw_data/customers")

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/workspace/raw_schema/rawvolume/raw_data/airports")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.silver_schema

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`/Volumes/workspace/bronze/bronzevolume/flights/data/`

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS `workspace`.`silver`.`silver_business`;
# MAGIC

# COMMAND ----------


spark.sql("DROP TABLE IF EXISTS silver.silver_business")


# COMMAND ----------

