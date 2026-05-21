# Bai 8: Hieu suat giao hang
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("bai08_HieuSuatGiaoHang").master("local[*]").getOrCreate()

orders_df = spark.read \
    .option("header", "true").option("inferSchema", "true") \
    .option("delimiter", ";").option("timestampFormat", "yyyy-MM-dd HH:mm") \
    .csv("data/Orders.csv")

order_items_df = spark.read \
    .option("header", "true").option("inferSchema", "true") \
    .option("delimiter", ";").option("timestampFormat", "yyyy-MM-dd HH:mm") \
    .csv("data/Order_Items.csv")

joined = orders_df.join(order_items_df, "Order_ID") \
    .select(
        orders_df["Order_ID"],
        orders_df["Order_Delivered_Carrier_Date"],
        order_items_df["Shipping_Limit_Date"]
    )

delivery_df = joined \
    .filter(col("Order_Delivered_Carrier_Date").isNotNull()) \
    .filter(col("Shipping_Limit_Date").isNotNull()) \
    .withColumn("ChenhLech_Ngay",
        datediff(col("Order_Delivered_Carrier_Date"), col("Shipping_Limit_Date")))

stats = delivery_df.agg(
    count("Order_ID").alias("TongSo"),
    round(avg("ChenhLech_Ngay"), 2).alias("ChenhLech_TB"),
    min("ChenhLech_Ngay").alias("SomNhat"),
    max("ChenhLech_Ngay").alias("TreNhat")
).first()

print("=" * 40)
print("=== HIEU SUAT GIAO HANG ===")
print("=" * 40)
print(f"Tong so don da giao: {stats['TongSo']}")
print(f"Chenh lech trung binh (ngay): {stats['ChenhLech_TB']}")
print(f"Giao som nhat (ngay): {stats['SomNhat']}")
print(f"Giao tre nhat (ngay): {stats['TreNhat']}")

print("\nPhan loai giao hang:")
phan_loai = delivery_df \
    .withColumn("Loai",
        when(col("ChenhLech_Ngay") == 0, "Dung han")
        .when(col("ChenhLech_Ngay") < 0, "Giao som")
        .otherwise("Giao tre")) \
    .groupBy("Loai") \
    .agg(count("Order_ID").alias("So_Luong"))
phan_loai.show(3, truncate=False)

spark.stop()
