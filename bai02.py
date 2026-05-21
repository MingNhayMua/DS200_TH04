# Bai 2: Thong ke tong so don hang, khach hang, nguoi ban
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("bai02_ThongKe").master("local[*]").getOrCreate()

orders_df = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv("data/Orders.csv")
customers_df = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv("data/Customer_List.csv")
order_items_df = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv("data/Order_Items.csv")

total_orders = orders_df.select("Order_ID").distinct().count()
total_customers = customers_df.select("Customer_Trx_ID").distinct().count()
total_sellers = order_items_df.select("Seller_ID").distinct().count()

print("=" * 40)
print("=== THONG KE TONG QUAN")
print("=" * 40)
print(f"Tong so don hang: {total_orders}")
print(f"Tong so khach hang: {total_customers}")
print(f"Tong so nguoi ban: {total_sellers}")

spark.stop()
