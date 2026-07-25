import dlt
from pyspark.sql.functions import *
@dlt.view(
    name = "customer_gold_view"

)



def customer_gold_view():
    df = spark.readStream.table("customer_silver_view")
    return df



dlt.create_streaming_table(
name = "customer_gold"

)

dlt.create_auto_cdc_flow(
    target = "customer_gold",
    source = "customer_gold_view",
    keys = ["customer_id"],
    sequence_by = col("processdate"),
    stored_as_scd_type = 2,
    except_column_list=["processdate"]
)