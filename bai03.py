# Bai 3: Don hang theo quoc gia
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("bai03_DonHangTheoQuocGia").master("local[*]").getOrCreate()

orders_df = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv("data/Orders.csv")
customers_df = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv("data/Customer_List.csv")

joined = orders_df.join(customers_df, "Customer_Trx_ID").select("Order_ID", "Customer_Country")

result = joined.groupBy("Customer_Country") \
    .agg(count("Order_ID").alias("So_Don_Hang")) \
    .orderBy(col("So_Don_Hang").desc())

print("=" * 40)
print("=== SO LUONG DON HANG THEO QUOC GIA ===")
print("=" * 40)
result.show(30, truncate=False)

spark.stop()
