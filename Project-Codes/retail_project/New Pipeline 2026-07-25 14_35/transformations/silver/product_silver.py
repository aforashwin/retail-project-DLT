import dlt
from pyspark.sql.functions import *
@dlt.view(
    name = "product_silver_view"

)



def product_silver_view():
    df = spark.readStream.table("bronze_products")
    df = df.withColumn(
    "discount_price",
    round(col("price") * 0.90, 2))
    df = df.withColumn(
    "price_category",
    when(col("price") < 100, "Low")
    .when(col("price") < 300, "Medium")
    .otherwise("High"))
    df = df.withColumn("processdate", current_timestamp())

    return df



dlt.create_streaming_table(
name = "product_silver"

)

dlt.create_auto_cdc_flow(
    target = "product_silver",
    source = "product_silver_view",
    keys = ["product_id"],
    sequence_by = col("processdate"),
    stored_as_scd_type = 1
)