from pyspark.sql import SparkSession
from pyspark.sql.types import StringType


spark = SparkSession.builder \
    .appName("Customer Sales Data Transformation") \
    .getOrCreate()


file_path = "dbfs:/mnt/Bronze/sales_view/sales/20240107_sales_data.csv"
df = spark.read.format("csv").option("header", "true").load(file_path)


def camel_to_snake(camel_str):
    import re
    snake_str = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', camel_str)
    snake_str = re.sub('([a-z0-9])([A-Z])', r'\1_\2', snake_str).lower()
    return snake_str


df_snake_case = df.toDF(*[camel_to_snake(col_name) for col_name in df.columns])


silver_layer_path = "dbfs:/mnt/silver/sales_view/customer_sales"
table_name = "customer_sales"

df_snake_case.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{silver_layer_path}{table_name}")
