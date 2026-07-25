import dlt
from pyspark.sql.functions import *
@dlt.view(
    name = "product_gold_view"

)



def sales_gold_view():
    df = spark.readStream.table("product_silver_view")
    return df



dlt.create_streaming_table(
name = "product_gold"

)

dlt.create_auto_cdc_flow(
    target = "product_gold",
    source = "product_gold_view",
    keys = ["product_id"],
    sequence_by = col("processdate"),
    stored_as_scd_type = 2,
    except_column_list=["processdate"]
)