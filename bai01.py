# Bai 1: Doc du lieu & suy kieu du lieu tu dong
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("bai01_DocDuLieu").master("local[*]").getOrCreate()

file_names = ["Orders", "Customer_List", "Order_Items", "Products", "Order_Reviews"]
data_paths = [
    "data/Orders.csv",
    "data/Customer_List.csv",
    "data/Order_Items.csv",
    "data/Products.csv",
    "data/Order_Reviews.csv"
]

for i in range(len(file_names)):
    print("\n" + "=" * 40)
    print("=== " + file_names[i] + ".csv")
    print("=" * 40)

    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("delimiter", ";") \
        .csv(data_paths[i])

    print("\nSchema (tu suy kieu du lieu):")
    df.printSchema()

    print("\n5 dong du lieu dau tien:")
    df.show(5, truncate=False)

spark.stop()
