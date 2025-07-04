# Databricks notebook source
# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

#Catalog Name
catalog = "workspace"
#Cdc Column
cdc_col = "ModifiedDate"
#Backdated Refresh
backdated_refresh = ""
#source object
source_object = "silver_bookings"
#source schema
source_schema = "silver"

#Source Fact Table
fact_table = f"{catalog}.{source_schema}.{source_object}"

target_schema = "gold"

target_object = "FactBookings"

surrogate_key = "DimPassengersKey"

# Fact Key Columns list
fact_key_cols = ["DimPassengersKey", "dimFlightskey", "dimAirportskey", "booking_date"]

# COMMAND ----------

# Array of all the dimensions needed for the fact table
dimensions = [
    {
        "table":f"{catalog}.{target_schema}.dimPassengers",
        "alias":"DimPassengers",
        "join_keys":[("passenger_id", "passenger_id")] #(fact_col, dim_col)
    },
    {
        "table":f"{catalog}.{target_schema}.dimFlights",
        "alias":"DimFlights",
        "join_keys":[("flight_id", "flight_id")] #(fact_col, dim_col)
    },

    {
        "table":f"{catalog}.{target_schema}.dimAirports",
        "alias":"DimAiports",
        "join_keys":[("airport_id", "airport_id")] #(fact_col, dim_col)
    }

]



#Columns you want to keep from fact table (besides surrogate keys)
fact_columns = ["amount", "booking_date", " ModifiedDate"] #in fact table we only surrogate key and numeric columns /dimensionkey --->Concept of data warehouosing

# COMMAND ----------

# MAGIC %md
# MAGIC ## Last Load Date

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
  #Test the last load 
  last_load = backdated_refresh

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dynamic Fcat Query [bring keys]

# COMMAND ----------

def generate_fact_query_incremental(fact_table, dimensions, fact_columns, cdc_column, last_load):
    fact_alias = "f"

    # Base columns from fact table
    select_cols = [f"{fact_alias}.{col}" for col in fact_columns]

    # Build JOIN clauses dynamically
    join_clauses = []
    for dim in dimensions:
        table_full = dim["table"]
        alias = dim["alias"]
        table_name = table_full.split(".")[-1]
        surrogate_key_col = f"{alias}.{table_name}key"
        select_cols.append(surrogate_key_col)

        # Build ON conditions
        on_conditions = [
            f"{fact_alias}.{fk} = {alias}.{dk}"
            for fk, dk in dim["join_keys"]
        ]

        # Build join string and append to list
        join_clause = f"LEFT JOIN {table_full} AS {alias} ON " + " AND ".join(on_conditions)
        join_clauses.append(join_clause)

    # Final SELECT and JOIN clauses
    select_clause = ",\n  ".join(select_cols)
    joins = "\n".join(join_clauses)

    # Where clause for incremental filtering
    where_clause = f"{fact_alias}.{cdc_column} >= DATE('{last_load}')"

    # Final query
    query = f"""
    SELECT
      {select_clause}
    FROM {fact_table} AS {fact_alias}
    {joins}
    WHERE {where_clause}
    """.strip()

    return query


# COMMAND ----------

query = generate_fact_query_incremental(fact_table, dimensions, fact_columns, cdc_col, last_load)


# COMMAND ----------

# MAGIC %md
# MAGIC ## DF_FACT

# COMMAND ----------

df_fact = spark.sql(query)

# COMMAND ----------

df_fact.display()

# COMMAND ----------

# A few Checks
df_fact.groupBy('DimPassengerskey', 'DimFlightskey', 'DimAirportskey').count().filter("count>1").display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df_fact.filter((col('DimPassengersKey') == 189) & (col('DimFlightskey') == 8) & (col('DimAirportskey') ==44)).display()
# same passenger same flight same airport. therefore we need to include bookingdate in the key columns

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upsert

# COMMAND ----------

# MAGIC %md
# MAGIC ### In fact table key column(primary key) is combination of surrogate keys

# COMMAND ----------

# Fact key columns mERGE cONDITION
fact_key_cols_str = " AND ".join([f"src.{col} = trg.{col}" for col in fact_key_cols])
fact_key_cols_str

# COMMAND ----------

from delta.tables import DeltaTable



# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
    dlt_obj = DeltaTable.forName(spark, f"{catalog}.{target_schema}.{target_object}")
    dlt_obj.alias("trg").merge(df_fact.alias("src"), fact_key_cols_str)\
                        .whenMatchedUpdateAll(condition =f" src.{cdc_col}>= trg.{cdc_col}")\
                        .whenNotMatchedInsertAll()\
                        .execute()
else:
    df_fact.write.format("delta")\
            .mode("append")\
            .saveAsTable(f"{catalog}.{target_schema}.{target_object}")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.gold.factbookings

# COMMAND ----------

