import dlt
from pyspark.sql.functions import *
@dlt.view(
    name = "customer_silver_view"

)



def customer_silver_view():
    df = spark.readStream.table("bronze_customer")
    df = df.withColumn("name", upper(col("name")))
    df = df.withColumn("processdate", current_timestamp())

    return df



dlt.create_streaming_table(
name = "customer_silver"

)

dlt.create_auto_cdc_flow(
    target = "customer_silver",
    source = "customer_silver_view",
    keys = ["customer_id"],
    sequence_by = col("processdate"),
    stored_as_scd_type = 1
)