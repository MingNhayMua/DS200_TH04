# Bai 4: Don hang theo nam, thang
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("bai04_DonHangTheoNamThang").master("local[*]").getOrCreate()

orders_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("delimiter", ";") \
    .option("timestampFormat", "yyyy-MM-dd HH:mm") \
    .csv("data/Orders.csv")

result = orders_df \
    .withColumn("Nam", year(col("Order_Purchase_Timestamp"))) \
    .withColumn("Thang", month(col("Order_Purchase_Timestamp"))) \
    .groupBy("Nam", "Thang") \
    .agg(count("Order_ID").alias("So_Don_Hang")) \
    .orderBy(col("Nam").asc(), col("Thang").desc())

print("=" * 40)
print("=== SO LUONG DON HANG THEO NAM VA THANG ===")
print("=== (Nam tang dan, Thang giam dan) ===")
print("=" * 40)
result.show(50, truncate=False)

spark.stop()
