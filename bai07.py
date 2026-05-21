# Bai 7: San pham ban chay & diem danh gia TB
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("bai07_SanPhamBanChay").master("local[*]").getOrCreate()

order_items_df = spark.read.option("header", "true").option("inferSchema", "true") \
    .option("delimiter", ";").csv("data/Order_Items.csv")

reviews_df = spark.read.option("header", "true").option("inferSchema", "true") \
    .option("delimiter", ";").csv("data/Order_Reviews.csv")

# San pham ban chay nhat
product_sales = order_items_df.groupBy("Product_ID") \
    .agg(count("Order_Item_ID").alias("So_Luong_Ban")) \
    .orderBy(col("So_Luong_Ban").desc())

print("=" * 40)
print("=== SAN PHAM BAN CHAY NHAT ===")
print("=" * 40)
product_sales.show(10, truncate=False)

# Diem danh gia trung binh tung san pham
valid_reviews = reviews_df.filter(col("Review_Score").isNotNull()) \
    .filter(col("Review_Score").between(1, 5))

product_reviews = order_items_df.join(valid_reviews, "Order_ID") \
    .groupBy("Product_ID") \
    .agg(
        round(avg("Review_Score"), 2).alias("Diem_TB"),
        count("Review_ID").alias("So_Luong_Danh_Gia")
    ).orderBy(col("So_Luong_Danh_Gia").desc())

print("\n=== DIEM DANH GIA TRUNG BINH TUNG SAN PHAM ===")
print("(Top san pham co nhieu danh gia nhat)")
print("=" * 40)
product_reviews.show(10, truncate=False)

spark.stop()
