import dlt
from pyspark.sql.functions import *
@dlt.view(
    name = "sales_gold_view"

)



def sales_gold_view():
    df = spark.readStream.table("sales_silver_view")
    return df



dlt.create_streaming_table(
name = "sales_gold"

)

dlt.create_auto_cdc_flow(
    target = "sales_gold",
    source = "sales_gold_view",
    keys = ["sales_id"],
    sequence_by = col("processdate"),
    stored_as_scd_type = 2,
    except_column_list=["processdate"]
)