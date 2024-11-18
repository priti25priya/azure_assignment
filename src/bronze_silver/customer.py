from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, split, lit, when, to_date
from pyspark.sql.types import StringType


spark = SparkSession.builder \
    .appName("Customer Data Transformation") \
    .getOrCreate()



file_path = "dbfs:/mnt/Bronze/sales_view/customer/20240107_sales_customer.csv"
df = spark.read.format("csv").option("header", "true").load(file_path)


def camel_to_snake(camel_str):
    import re
    snake_str = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', camel_str)
    snake_str = re.sub('([a-z0-9])([A-Z])', r'\1_\2', snake_str).lower()
    return snake_str


@udf(StringType())
def convert_headers(header):
    return camel_to_snake(header)


df_snake_case = df.toDF(*[camel_to_snake(col_name) for col_name in df.columns])


df_snake_case = df_snake_case \
    .withColumn("first_name", split(col("name"), " ").getItem(0)) \
    .withColumn("last_name", split(col("name"), " ").getItem(1))


df_snake_case = df_snake_case.withColumn("domain", split(col("email"), "@").getItem(1).substr(1, 100).split("\.").getItem(0))


df_snake_case = df_snake_case.withColumn("gender", when(col("gender") == "Male", "M").when(col("gender") == "Female", "F"))


df_snake_case = df_snake_case \
    .withColumn("date", to_date(split(col("joining_date"), " ").getItem(0), "yyyy-MM-dd")) \
    .withColumn("time", split(col("joining_date"), " ").getItem(1))


df_snake_case = df_snake_case.withColumn("expenditure_status", when(col("spent") < 200, "MINIMUM").otherwise("MAXIMUM"))


silver_layer_path = "dbfs:/mnt/silver/sales_view/customer"
table_name = "customer"

df_snake_case.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{silver_layer_path}{table_name}")
