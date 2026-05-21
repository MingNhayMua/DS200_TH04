# Bai 6: Doanh thu 2024 theo danh muc san pham
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("bai06_DoanhThu2024").master("local[*]").getOrCreate()

orders_df = spark.read \
    .option("header", "true").option("inferSchema", "true") \
    .option("delimiter", ";").option("timestampFormat", "yyyy-MM-dd HH:mm") \
    .csv("data/Orders.csv")

order_items_df = spark.read.option("header", "true").option("inferSchema", "true") \
    .option("delimiter", ";").csv("data/Order_Items.csv")

products_df = spark.read.option("header", "true").option("inferSchema", "true") \
    .option("delimiter", ";").csv("data/Products.csv")

orders_2024 = orders_df.filter(year(col("Order_Purchase_Timestamp")) == 2024) \
    .select("Order_ID")

revenue_df = orders_2024.join(order_items_df, "Order_ID") \
    .join(products_df, "Product_ID") \
    .withColumn("DoanhThu", col("Price") + col("Freight_Value")) \
    .groupBy("Product_Category_Name") \
    .agg(round(sum("DoanhThu"), 2).alias("Tong_Doanh_Thu")) \
    .orderBy(col("Tong_Doanh_Thu").desc())

print("=" * 40)
print("=== DOANH THU NAM 2024 THEO DANH MUC ===")
print("=" * 40)
revenue_df.show(100, truncate=False)

spark.stop()
