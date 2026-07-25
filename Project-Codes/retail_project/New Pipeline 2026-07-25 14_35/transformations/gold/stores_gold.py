import dlt
from pyspark.sql.functions import *
@dlt.view(
    name = "stores_gold_view"

)



def sales_gold_view():
    df = spark.readStream.table("stores_silver_view")
    return df



dlt.create_streaming_table(
name = "stores_gold"

)

dlt.create_auto_cdc_flow(
    target = "stores_gold",
    source = "stores_gold_view",
    keys = ["store_id"],
    sequence_by = col("processdate"),
    stored_as_scd_type = 2,
    except_column_list=["processdate"]
)