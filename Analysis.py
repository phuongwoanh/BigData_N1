import findspark

findspark.init()

from pyspark.sql import SparkSession

# 1. Khởi tạo SparkSession kết nối với Spark Master
spark = SparkSession.builder \
    .appName("Phan_Tich_Hanh_Vi_TMDT") \
    .master("spark://26.37.93.102:7077") \
    .config("spark.executor.memory", "3g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("Đã khởi tạo thành công \n")

# 2. Đọc dữ liệu từ HDFS (Đã bỏ header và inferSchema vì Parquet không cần)
df = spark.read.parquet("hdfs://master:9000/data/test.parquet")

print("--- CẤU TRÚC DỮ LIỆU ---")
df.printSchema()

print("--- 20 DÒNG DỮ LIỆU ĐẦU TIÊN ---")
df.show(20)

# 5. Đếm tổng số dòng
total_rows = df.count()
print(f"Tổng số bản ghi: {total_rows}")

# 6. Dừng phiên làm việc
spark.stop()