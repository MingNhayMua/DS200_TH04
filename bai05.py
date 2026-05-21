# Bai 5: Thong ke diem danh gia
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName("bai05_DanhGia").master("local[*]").getOrCreate()

reviews_df = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv("data/Order_Reviews.csv")

# Cast Review_Score sang IntegerType de xu ly cac gia tri string (vi du date)
# try_cast tra ve NULL neu khong cast duoc
reviews_df = reviews_df.withColumn("Review_Score_int", col("Review_Score").try_cast(IntegerType()))

# Xu ly NULL va gia tri ngoai le
clean_df = reviews_df.filter(col("Review_Score_int").isNotNull()) \
    .filter(col("Review_Score_int").between(1, 5))

total_valid = clean_df.count()
total_all = reviews_df.count()
total_invalid = total_all - total_valid

print("=" * 40)
print("=== THONG KE DIEM DANH GIA ===")
print("=" * 40)
print(f"So luong danh gia hop le (1-5): {total_valid}")
print(f"So luong danh gia NULL/ngoai le: {total_invalid}")

avg_score = clean_df.agg(avg("Review_Score_int").alias("DiemTB")).first()["DiemTB"]
print(f"\nDiem danh gia trung binh: {avg_score:.2f}")

print("\nSo luong danh gia theo tung muc diem:")
score_count = clean_df.groupBy("Review_Score_int") \
    .agg(count("Review_ID").alias("So_Luong")) \
    .orderBy("Review_Score_int")
score_count.show(5, truncate=False)

spark.stop()
