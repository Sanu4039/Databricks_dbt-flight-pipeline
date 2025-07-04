# Databricks notebook source
# df = spark.read.format("delta")\
#     .load("/Volumes/workspace/bronze/bronzevolume/bookings/data/")
# display(df)

# COMMAND ----------

# from pyspark.sql.functions import *
# from pyspark.sql.types import *

# COMMAND ----------

# df = df.withColumn("amount",col("amount").cast(DoubleType()))\
#     .withColumn("ModifiedDate",current_timestamp())\
#     .withColumn("booking_date",to_date(col("booking_date")))\
#     .drop("_rescued_data")

# display(df)


# COMMAND ----------


import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

@dlt.table(
    name="stage_bookings")
def stage_bookings():
    df = spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronzevolume/bookings/data/")
    return df
# simply load the table incrementaly

# COMMAND ----------

@dlt.view(
    name = "transf_booking" )
def transf_booking():
    df = dlt.read("stage_bookings")
    df = spark.readStream.table("stage_bookings")
    df = df.withColumn("amount",col("amount").cast(DoubleType()))\
    .withColumn("ModifiedDate",current_timestamp())\
    .withColumn("booking_date",to_date(col("booking_date")))\
    .drop("_rescued_data")

display(df)

# COMMAND ----------

rules = {
    "rule1":"booking_id is NOT NULL",
    "rule2":"passenger_id is NOT NULL"
}

# COMMAND ----------

@dlt.table(
    name = "silver_bookings"
)
@dlt.expect_all_drop(rules)
def silver_bookings():
    df = dlt.read("transf_booking") # can also use df = spark.readStream.table(tranf_bookings)
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Flights Dimension

# COMMAND ----------

df = spark.read.format("delta").load("/Volumes/workspace/bronze/bronzevolume/flights/data/")
display(df)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = df.withColumn("flight_date",to_date(col("flight_date")))\
    .withColumn("ModifiedDate",current_timestamp())\
    .drop("_rescued_data")

display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Passenger Dimension
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.format("delta").load("/Volumes/workspace/bronze/bronzevolume/customers/data/")
display(df)

# COMMAND ----------

df = df.withColumn("ModifiedDate",current_timestamp())\
    .drop("_rescued_data")

display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Airports Data

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.format("delta").load("/Volumes/workspace/bronze/bronzevolume/airports/data/")
display(df)

# COMMAND ----------

df = df.withColumn("ModifiedDate",current_timestamp())\
    .drop("_rescued_data")

display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Analysing the Data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.silver.silver_airports

# COMMAND ----------

