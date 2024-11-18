from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType


spark = SparkSession.builder \
    .appName("Store Product Sales Analysis") \
    .getOrCreate()


product_table_path = 'dbfs:/mnt/silver/sales_view/product'
store_table_path = 'dbfs:/mnt/silver/sales_view/store'
customer_sales_path = 'dbfs:/mnt/silver/sales_view/customer_sales'


product_df = spark.read.format("delta").load(product_table_path)
store_df = spark.read.format("delta").load(store_table_path)
customer_sales_df = spark.read.format("delta").load(customer_sales_path)



product_store_df = product_df.join(
    store_df,
    product_df["store_id"] == store_df["store_id"],
    "inner"
).select(
    store_df["store_id"], store_df["store_name"], store_df["location"], store_df["manager_name"],
    product_df["product_name"], product_df["product_code"], product_df["description"],
    product_df["category_id"], product_df["price"], product_df["stock_quantity"], product_df["supplier_id"],
    product_df["created_at"].alias("product_created_at"), product_df["updated_at"].alias("product_updated_at"),
    product_df["image_url"], product_df["weight"], product_df["expiry_date"], product_df["is_active"],
    product_df["tax_rate"]
)


final_df = customer_sales_df.join(
    product_store_df,
    customer_sales_df["product_id"] == product_store_df["product_code"],
    "inner"
).select(
    customer_sales_df["order_date"], customer_sales_df["category"], customer_sales_df["city"],
    customer_sales_df["customer_id"], customer_sales_df["order_id"], customer_sales_df["product_id"],
    customer_sales_df["profit"], customer_sales_df["region"], customer_sales_df["sales"],
    customer_sales_df["segment"], customer_sales_df["ship_date"], customer_sales_df["ship_mode"],
    customer_sales_df["latitude"], customer_sales_df["longitude"],
    product_store_df["store_name"], product_store_df["location"], product_store_df["manager_name"],
    product_store_df["product_name"], product_store_df["price"], product_store_df["stock_quantity"],
    product_store_df["image_url"]
)


gold_layer_path = "dbfs:/mnt/gold/sales_view/StoreProductSalesAnalysis"
table_name = "StoreProductSalesAnalysis"

final_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{gold_layer_path}{table_name}")
