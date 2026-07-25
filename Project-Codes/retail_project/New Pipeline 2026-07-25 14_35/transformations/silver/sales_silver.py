import dlt
from pyspark.sql.functions import *
@dlt.view(
    name = "sales_silver_view"

)



def stores_silver_view():
    df = spark.readStream.table("bronze_sales")
    df = df.withColumn(
    "net_amount",
    round(col("total_amount") - col("discount"), 2)
)
    df = df.withColumn("processdate", current_timestamp())

    return df



dlt.create_streaming_table(
name = "sales_silver"

)

dlt.create_auto_cdc_flow(
    target = "sales_silver",
    source = "sales_silver_view",
    keys = ["sales_id"],
    sequence_by = col("processdate"),
    stored_as_scd_type = 1
)