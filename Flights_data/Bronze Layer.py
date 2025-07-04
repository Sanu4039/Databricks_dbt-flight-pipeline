# Databricks notebook source
# MAGIC %md
# MAGIC ###  Incremental Data Ingestion

# COMMAND ----------

# %sql
# CREATE Volume workspace.raw_schema.Golden

# COMMAND ----------

# %sql
# CREATE SCHEMA workspace.bronze

# COMMAND ----------

# %sql
# CREATE VOLUME workspace.bronze.bronzevolume

# COMMAND ----------

# %sql
# CREATE VOLUME workspace.silver.silvervolume

# COMMAND ----------

# %sql
# CREATE VOLUME workspace.gold.goldvolume

# COMMAND ----------

# df = spark.readStream.format("cloudFiles")\
#     .option("cloudFiles.format", "csv")\
#     .option("cloudFiles.schemaLocation","/Volumes/workspace/bronze/bronzevolume/bookings/checkpoint")\
#     .option("cloudFiles.schemaEvolutionMode", "rescue")\
#     .option("cloudFiles.maxFilesPerTrigger", 1)\
#     .load("/Volumes/workspace/raw_schema/rawvolume/raw_data/bookings/")
    

# COMMAND ----------

# df.writeStream.format("delta")\
#     .outputMode("append")\
#     .trigger(once=True)\
#     .option("checkpointLocation", "/Volumes/workspace/bronze/bronzevolume/bookings/checkpoint")\
#     .option("path", "/Volumes/workspace/bronze/bronzevolume/bookings/data")\
#     .start()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from delta.`/Volumes/workspace/bronze/bronzevolume/bookings/data/`

# COMMAND ----------

dbutils.widgets.text("src","")

# COMMAND ----------

src_value = dbutils.widgets.get("src")


# COMMAND ----------

df = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format", "csv")\
    .option("cloudFiles.schemaLocation",f"/Volumes/workspace/bronze/bronzevolume/{src_value}/checkpoint")\
    .option("cloudFiles.schemaEvolutionMode", "rescue")\
    .option("cloudFiles.maxFilesPerTrigger", 1)\
    .load(f"/Volumes/workspace/raw_schema/rawvolume/raw_data/{src_value}/")
    

# COMMAND ----------

df.writeStream.format("delta")\
    .outputMode("append")\
    .trigger(once=True)\
    .option("checkpointLocation", f"/Volumes/workspace/bronze/bronzevolume/{src_value}/checkpoint")\
    .option("path", f"/Volumes/workspace/bronze/bronzevolume/{src_value}/data")\
    .start()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`/Volumes/workspace/bronze/bronzevolume/customers/data`

# COMMAND ----------

