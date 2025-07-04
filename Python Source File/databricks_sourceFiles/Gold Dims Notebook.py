# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM `workspace`.`silver`.`silver_flights`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM `workspace`.`silver`.`silver_passengers`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parameters

# COMMAND ----------

# #KEY COLUMN
# dbutils.widgets.text("keycols","")

# #CDC COLUMN
# dbutils.widgets.text("cdccol","")

# #Back-Dated Refresh
# dbutils.widgets.text("backdated_refresh","")

# #source Object
# dbutils.widgets.text("source_object","")

# #source Schema
# dbutils.widgets.text("source_schema","")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fetching Parameters and creating variables

# COMMAND ----------

# #key_cols_lis
# key_cols_list = eval(dbutils.widgets.get("keycols"))

# #CDC col
# cdc_col = dbutils.widgets.get("cdccol")

# #backdated refresh
# backdated_refresh = dbutils.widgets.get("backdated_refresh")

# #source object
# source_object = dbutils.widgets.get("source_object")

# #source schema
# source_schema = dbutils.widgets.get("source_schema")

# COMMAND ----------

###Flight

# catalog = "workspace"

# key_cols = "['flight_id']"
# key_cols_list = eval(key_cols)

# cdc_col = "ModifiedDate"

# backdated_refresh = ""

# source_object = "silver_flights"

# source_schema = "silver"

# target_schema = "gold"

# target_object = "DimFlights"

# surrogate_key = "DimFlightsKey"

# COMMAND ----------

### Airport

# catalog = "workspace"

# key_cols = "['airport_id']"
# key_cols_list = eval(key_cols)

# cdc_col = "ModifiedDate"

# backdated_refresh = ""

# source_object = "silver_airports"

# source_schema = "silver"

# target_schema = "gold"

# target_object = "DimAirports"

# surrogate_key = "DimAirportsKey"

# COMMAND ----------

### Passengers

catalog = "workspace"

key_cols = "['passenger_id']"
key_cols_list = eval(key_cols)

cdc_col = "ModifiedDate"

backdated_refresh = ""

source_object = "silver_passengers"

source_schema = "silver"

target_schema = "gold"

target_object = "DimPassengers"

surrogate_key = "DimPassengersKey"

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Incremental data ingestion

# COMMAND ----------

# MAGIC %md
# MAGIC ### Last Load Date

# COMMAND ----------

#No BackDated Refresh
if len(backdated_refresh)==0:
  #if dbutils.fs.ls(f"")---> # CANNOT USE THIS BECAUSE OUR VOLUME IS MANGED VOLUME AND WE CANNOT PROVIDE THE LOCATION
  
  #if data exists in the destination
  if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
    last_load = spark.sql(f"select max({cdc_col}) from {catalog}.{target_schema}.{target_object}").collect()[0][0]  #collect method simply convert the data into list and within that list we are grabbing 0th and 0th object.
  else:
    # Yes back dated refresh
    last_load = "1900-01-01 00:00:00"
else:
  last_load = backdated_refresh

# COMMAND ----------

last_load

# COMMAND ----------

df_src = spark.sql(f"SELECT * FROM {source_schema}.{source_object} where {cdc_col}> '{last_load}'")

# COMMAND ----------

df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Old VS New

# COMMAND ----------

key_cols_string_incremental = ",".join(key_cols_list )


# COMMAND ----------

# if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
#   # Key column string for INCREMENTAL
#   key_cols_string = ",".join(key_cols_list )

#   df_trg = spark.sql(f"SELECT {key_cols_string_incremental}, {surrogate_key},create_date, update_date FROM {catalog}.{target_schema}.{target_object}")  
                      

# else:
#   # Key columns string for Initial Load
#   key_cols_string_init  = [f"'' AS {i}"for i in key_cols_list]
#   key_cols_string_init = ",".join(key_cols_string_init)   #unlist the list, use join, whole list becomes
#   df_trg = spark.sql(f """SELECT {key_cols_string_init}, '' AS {surrogate_key},  CAST('1900-01-01 00:00:00' AS timestamp) AS create_date, CAST('1900-01-01 00:00:00' AS timestamp) AS update_date WHERE 1=0 """)  #1=0 because there is no table.
                           
if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
    # Key column string for INCREMENTAL
    key_cols_string_incremental = ",".join(key_cols_list)  # make sure this is defined

    df_trg = spark.sql(f"""
        SELECT {key_cols_string_incremental}, {surrogate_key}, create_date, update_date
        FROM {catalog}.{target_schema}.{target_object}
    """)
else:
    # Key columns string for Initial Load
    key_cols_string_init = [f"'' AS {i}" for i in key_cols_list]
    key_cols_string_init = ",".join(key_cols_string_init)


    df_trg = spark.sql(f"""
        SELECT {key_cols_string_init},
               CAST('0' AS INT) AS {surrogate_key},
               CAST('1900-01-01 00:00:00' AS timestamp) AS create_date,
               CAST('1900-01-01 00:00:00' AS timestamp) AS update_date
        FROM (SELECT 1) AS dummy WHERE 1=0
    """)


# COMMAND ----------

df_trg.display()

# COMMAND ----------

# key_cols_string_incremental = ",".join(key_cols_list ) #used above


# COMMAND ----------

# spark.sql(f"SELECT {key_cols_string} FROM {catalog}.{source_schema}.{source_object}") #used above

# COMMAND ----------

# spark.sql(f"select '' AS flight_id, '' AS DimFlightsKey, '1900-01-01 00:00:00' AS create_date, '1900-01-01 00:00:00' AS update_date FROM workspace.silver.silver_flights").display() #used above

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join Conditions

# COMMAND ----------

join_condition = 'AND'.join([f"src.{i} = trg.{i}" for i in key_cols_list] )

# COMMAND ----------

df_src.createOrReplaceTempView("src")
df_trg.createOrReplaceTempView("trg")

df_join = spark.sql(f"""
          select src.*,
                 trg.{surrogate_key},
                 trg.create_date,
                 trg.update_date
          from src
          left join trg
          on {join_condition}
                 
          """)

# COMMAND ----------

df_join.display()

# COMMAND ----------

df_old = df_join.filter(col(f'{surrogate_key}').isNotNull())
df_old.display()
df_new = df_join.filter(col(f'{surrogate_key}').isNull())
df_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ENRICHGING DATAFRAME

# COMMAND ----------

# MAGIC %md
# MAGIC #### Preparing DF_OLD

# COMMAND ----------

df_old_enr = df_old.withColumn("update_date", current_timestamp())


# COMMAND ----------

# MAGIC %md
# MAGIC ### Preparing DF_NEW

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
    max_surrogate_key = spark.sql(f"""
                            select max({surrogate_key}) from {catalog}.{target_schema}.{target_object}
                            """).collect()[0][0]
    df_new_enr = df_new.withColumn(f'{surrogate_key}', lit(max_surrogate_key)+lit(1)+ monotonically_increasing_id())\
        .withColumn("create_date", current_timestamp())\
        .withColumn("update_date", current_timestamp())

else:
    max_surrogate_key = 0
    #lit is juts for literal value
    df_new_enr = df_new.withColumn(f'{surrogate_key}', lit(max_surrogate_key)+lit(1)+ monotonically_increasing_id())\
        .withColumn("create_date", current_timestamp())\
        .withColumn("update_date", current_timestamp())


# COMMAND ----------

df_new_enr.display()
df_old_enr.display()

# COMMAND ----------

#MAX_SURROGATE_KEY WHEN TABLE IS READY
    # spark.sql("""
    #     select max({surrogate_key}) from {catalog}.{target_schema}.{target_object}
    # """).collect()[0][0]

# COMMAND ----------

# MAGIC %md
# MAGIC ### UNION new and old

# COMMAND ----------

df_union = df_old_enr.unionByName(df_new_enr)



# COMMAND ----------

df_union.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## UPSERT

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
    dlt_obj = DeltaTable.forName(spark, f"{catalog}.{target_schema}.{target_object}")
    dlt_obj.alias("trg").merge(df_union.alias("src"), f"trg.{surrogate_key} = src.{surrogate_key}")\
                        .whenMatchedUpdateAll(condition =f" src.{cdc_col}>= trg.{cdc_col}")\
                        .whenNotMatchedInsertAll()\
                        .execute()
else:
    df_union.write.format("delta")\
            .mode("append")\
            .saveAsTable(f"{catalog}.{target_schema}.{target_object}")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.dimairports

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.dimpassengers
# MAGIC     
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- upserted data
# MAGIC select * from workspace.silver.silver_passengers where passenger_id = 'P0049' 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- upserted data
# MAGIC select * from workspace.gold.dimpassengers where passenger_id = 'P0049' --dimensionkey is still the same though name is changed

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.silver.silver_airports where airport_id = 'A032' 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.gold.dimairports where airport_id = 'A032' 

# COMMAND ----------

