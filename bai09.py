# Bai 9: Phan nhom khach hang
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("bai09_PhanNhomKhachHang").master("local[*]").getOrCreate()

orders_df = spark.read \
    .option("header", "true").option("inferSchema", "true") \
    .option("delimiter", ";").option("timestampFormat", "yyyy-MM-dd HH:mm") \
    .csv("data/Orders.csv")

order_items_df = spark.read.option("header", "true").option("inferSchema", "true") \
    .option("delimiter", ";").csv("data/Order_Items.csv")

# Doc Customer_List de lay Subscriber_ID (dai dien cho 1 khach hang thuc)
customer_df = spark.read.option("header", "true").option("inferSchema", "true") \
    .option("delimiter", ";").csv("data/Customer_List.csv") \
    .select("Customer_Trx_ID", "Subscriber_ID")

# Tinh gia tri don hang
order_value_df = order_items_df \
    .groupBy("Order_ID") \
    .agg(round(sum(col("Price") + col("Freight_Value")), 2).alias("Order_Value"))

# Cast Order_Purchase_Timestamp sang timestamp neu can
orders_df = orders_df.withColumn("Purchase_TS", to_timestamp(col("Order_Purchase_Timestamp")))

# Join orders voi order_value va customer de lay Subscriber_ID
customer_orders = orders_df.join(order_value_df, "Order_ID") \
    .join(customer_df, "Customer_Trx_ID") \
    .select("Subscriber_ID", "Order_ID", "Order_Value", "Purchase_TS")

# Nhom theo Subscriber_ID (khach hang thuc su, co the co nhieu don)
customer_segments = customer_orders.groupBy("Subscriber_ID") \
    .agg(
        count("Order_ID").alias("So_Don_Hang"),
        round(avg("Order_Value"), 2).alias("Gia_Tri_TB"),
        datediff(max("Purchase_TS"), min("Purchase_TS")).alias("Ngay_ChenhLech")
    ).filter(col("So_Don_Hang") > 1) \
    .withColumn("Tan_Suat_Ngay",
        round(col("Ngay_ChenhLech") / (col("So_Don_Hang") - 1), 1))

# Lay gia tri trung binh
avg_row = customer_segments.agg(
    coalesce(avg("Gia_Tri_TB"), lit(0.0)).alias("avg_val"),
    coalesce(avg("Tan_Suat_Ngay"), lit(0.0)).alias("avg_freq")
).first()

avg_order_value = avg_row["avg_val"] if avg_row["avg_val"] is not None else 0.0
avg_frequency = avg_row["avg_freq"] if avg_row["avg_freq"] is not None else 0.0

result = customer_segments.withColumn("Nhom",
    concat(
        when(col("So_Don_Hang") >= 5, "Nhieu don").otherwise("It don"),
        lit(", "),
        when(col("Gia_Tri_TB") >= avg_order_value, "Gia tri cao").otherwise("Gia tri thap"),
        lit(", "),
        when(col("Tan_Suat_Ngay") <= avg_frequency, "Mua thuong xuyen").otherwise("Mua khong thuong xuyen")
    ))

print("=" * 40)
print("=== PHAN NHOM KHACH HANG ===")
print("=" * 40)
print(f"Gia tri TB cua don hang: {avg_order_value:.2f}")
print(f"Tan suat mua TB (ngay): {avg_frequency:.1f}")

print("\nMau du lieu phan nhom khach hang:")
result.select("Subscriber_ID", "So_Don_Hang", "Gia_Tri_TB", "Tan_Suat_Ngay", "Nhom").show(20, truncate=False)

print("\nSo luong khach hang theo nhom:")
result.groupBy("Nhom").agg(count("Subscriber_ID").alias("So_Luong")) \
    .orderBy(col("So_Luong").desc()).show(20, truncate=False)

spark.stop()
