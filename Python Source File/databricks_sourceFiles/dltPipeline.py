import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# ─────────────────────────────────────────────
# BOOKINGS
# ─────────────────────────────────────────────

@dlt.table(name="stage_bookings")
def stage_bookings():
    df = spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronzevolume/bookings/data/")
    return df

@dlt.view(name="transf_bookings")
def transf_bookings():
    df = spark.readStream.table("stage_bookings")\
        .withColumn("amount", col("amount").cast(DoubleType()))\
        .withColumn("ModifiedDate", current_timestamp())\
        .withColumn("booking_date", to_date(col("booking_date")))\
        .drop("_rescued_data")
    return df

rules = {
    "rule1": "booking_id is NOT NULL",
    "rule2": "passenger_id is NOT NULL"
}

@dlt.table(name="silver_bookings")
@dlt.expect_all_or_drop(rules)
def silver_bookings():
    df = spark.readStream.table("transf_bookings")\
        .withColumn("ModifiedDate", current_timestamp())\
        .drop("_rescued_data")
    return df

# ─────────────────────────────────────────────
# FLIGHTS
# ─────────────────────────────────────────────

df = spark.read.format("delta").load("/Volumes/workspace/bronze/bronzevolume/flights/data/")
display(df)

df = df.withColumn("flight_date", to_date(col("flight_date")))\
    .withColumn("ModifiedDate", current_timestamp())\
    .drop("_rescued_data")
display(df)

@dlt.view(name="transf_flights")
def transf_flights():
    df = spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronzevolume/flights/data/")\
        .withColumn("flight_date", to_date(col("flight_date")))\
        .withColumn("ModifiedDate", current_timestamp())\
        .drop("_rescued_data")
    return df

dlt.create_streaming_table("silver_flights")

dlt.create_auto_cdc_flow(
    target="silver_flights",
    source="transf_flights",
    keys=["flight_id"],
    sequence_by=col("ModifiedDate"),
    stored_as_scd_type=1
)

# ─────────────────────────────────────────────
# PASSENGERS
# ─────────────────────────────────────────────

df = spark.read.format("delta").load("/Volumes/workspace/bronze/bronzevolume/customers/data/")
display(df)

df = df.withColumn("ModifiedDate", current_timestamp())\
    .drop("_rescued_data")
display(df)

@dlt.view(name="transf_passengers")
def transf_passengers():
    df = spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronzevolume/customers/data/")\
        .withColumn("ModifiedDate", current_timestamp())\
        .drop("_rescued_data")
    return df

dlt.create_streaming_table("silver_passengers")

dlt.create_auto_cdc_flow(
    target="silver_passengers",
    source="transf_passengers",
    keys=["passenger_id"],
    sequence_by=col("ModifiedDate"),
    stored_as_scd_type=1
)

# ─────────────────────────────────────────────
# AIRPORTS
# ─────────────────────────────────────────────

df = spark.read.format("delta").load("/Volumes/workspace/bronze/bronzevolume/airports/data/")
display(df)

df = df.withColumn("ModifiedDate", current_timestamp())\
    .drop("_rescued_data")
display(df)

@dlt.view(name="transf_airports")
def transf_airports():
    df = spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronzevolume/airports/data/")\
        .withColumn("ModifiedDate", current_timestamp())\
        .drop("_rescued_data")
    return df

dlt.create_streaming_table("silver_airports")

dlt.create_auto_cdc_flow(
    target="silver_airports",
    source="transf_airports",
    keys=["airport_id"],
    sequence_by=col("ModifiedDate"),
    stored_as_scd_type=1
)

# ─────────────────────────────────────────────
# SILVER BUSINESS VIEW
# ─────────────────────────────────────────────

@dlt.table(name="silver_business")
def silver_business():
    bookings_df = dlt.readStream("silver_bookings").drop("_rescued_data")
    flights_df = dlt.readStream("silver_flights").drop("_rescued_data")
    passengers_df = dlt.readStream("silver_passengers").drop("_rescued_data")
    airports_df = dlt.readStream("silver_airports").drop("_rescued_data")

    df = bookings_df\
        .join(flights_df, ["flight_id"])\
        .join(passengers_df, ["passenger_id"])\
        .join(airports_df, ["airport_id"])\
        .drop("ModifiedDate")

    return df