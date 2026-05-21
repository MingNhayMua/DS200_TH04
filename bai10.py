# Bai 10: Xep hang seller
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("bai10_XepHangSeller").master("local[*]").getOrCreate()

order_items_df = spark.read.option("header", "true").option("inferSchema", "true") \
    .option("delimiter", ";").csv("data/Order_Items.csv")

seller_stats = order_items_df \
    .withColumn("DoanhThu", col("Price") + col("Freight_Value")) \
    .groupBy("Seller_ID") \
    .agg(
        round(sum("DoanhThu"), 2).alias("Tong_Doanh_Thu"),
        count_distinct("Order_ID").alias("So_Don_Hang")
    ) \
    .orderBy(col("Tong_Doanh_Thu").desc())

window_spec = Window.orderBy(col("Tong_Doanh_Thu").desc())
result = seller_stats.withColumn("Xep_Hang", row_number().over(window_spec))

print("=" * 40)
print("=== XEP HANG SELLER THEO DOANH THU ===")
print("=" * 40)
result.show(10, truncate=False)

spark.stop()
