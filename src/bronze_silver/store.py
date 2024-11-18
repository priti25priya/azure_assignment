from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, to_date
from pyspark.sql.types import StringType


spark = SparkSession.builder \
    .appName("Store Data Transformation") \
    .getOrCreate()


file_path = "dbfs:/mnt/Bronze/sales_view/store/20240107_sales_store.csv"
df = spark.read.format("csv").option("header", "true").load(file_path)


def camel_to_snake(camel_str):
    import re
    snake_str = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', camel_str)
    snake_str = re.sub('([a-z0-9])([A-Z])', r'\1_\2', snake_str).lower()
    return snake_str


df_snake_case = df.toDF(*[camel_to_snake(col_name) for col_name in df.columns])


df_transformed = df_snake_case.withColumn(
    "store_category",
    split(split(col("email"), "@").getItem(1), "\\.").getItem(0)
)


df_transformed = df_transformed \
    .withColumn("created_at", to_date(col("created_at"), "yyyy-MM-dd")) \
    .withColumn("updated_at", to_date(col("updated_at"), "yyyy-MM-dd"))


silver_layer_path = "dbfs:/mnt/silver/sales_view/store"
table_name = "store"

df_transformed.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{silver_layer_path}{table_name}")
