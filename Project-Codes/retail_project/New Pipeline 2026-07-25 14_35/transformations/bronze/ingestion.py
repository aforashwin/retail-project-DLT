import dlt

@dlt.table(
    name = "bronze_customer"
)

def bronze_customer():
    df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "csv")\
    .load("/Volumes/dlt_project/landing/bronze_volume/customers/")
    return df



@dlt.table(
 name = "bronze_products"
)

def bronze_products():

    df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "csv")\
    .load("/Volumes/dlt_project/landing/bronze_volume/products/")
    return df



@dlt.table(
    name = "bronze_sales"
)

def bronze_sales():
    df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "csv")\
    .load("/Volumes/dlt_project/landing/bronze_volume/sales/")
    return df





@dlt.table(
    name = "bronze_stores"

)

def bronze_stores():

    df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "csv")\
    .load("/Volumes/dlt_project/landing/bronze_volume/stores/")

    return df

