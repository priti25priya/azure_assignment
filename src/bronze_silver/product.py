from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import StringType


spark = SparkSession.builder \
    .appName("Product Data Transformation") \
    .getOrCreate()


file_path = "dbfs:/mnt/Bronze/sales_view/product/20240107_sales_product.csv"
df = spark.read.format("csv").option("header", "true").load(file_path)


def camel_to_snake(camel_str):
    import re
    snake_str = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', camel_str)
    snake_str = re.sub('([a-z0-9])([A-Z])', r'\1_\2', snake_str).lower()
    return snake_str


df_snake_case = df.toDF(*[camel_to_snake(col_name) for col_name in df.columns])


df_transformed = df_snake_case.withColumn(
    "sub_category",
    when(col("category_id") == 1, "phone")
    .when(col("category_id") == 2, "laptop")
    .when(col("category_id") == 3, "playstation")
    .when(col("category_id") == 4, "e-device")
    .otherwise("unknown")
)


silver_layer_path = "dbfs:/mnt/silver/sales_view/product"
table_name = "product"

df_transformed.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{silver_layer_path}{table_name}")
