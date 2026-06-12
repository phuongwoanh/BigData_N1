import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType


# 1. Khởi tạo SparkSession kết nối với Spark Master
spark = SparkSession.builder \
    .appName("Phan_Tich_Hanh_Vi_TMDT") \
    .master("spark://26.37.93.102:7077") \
    .config("spark.executor.memory", "3g") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("Đã khởi tạo thành công \n")

# 2. Đọc dữ liệu từ HDFS
df = spark.read.parquet("hdfs://master:9000/data/test.parquet")


print("--- CẤU TRÚC DỮ LIỆU ---")
df.printSchema()


print("--- 20 DÒNG DỮ LIỆU ĐẦU TIÊN ---")
df.show(20)


# 5. Đếm tổng số dòng
total_rows = df.count()
print(f"Tổng số bản ghi: {total_rows}")

#TIỀN XỬ LÝ DỮ LIỆU
# Xử lý duplicate dữ liệu
df = df.dropDuplicates()


#Đổi kiểu dữ liệu của cột Price
df = df.withColumn("price", F.col("price").cast(FloatType()))


# Điền bằng median price của cùng product_id thay vì xóa để giữ lại hành vi user
median_by_product = (
    df.filter(F.col("price") > 0)
    .groupBy("product_id")
    .agg(F.percentile_approx("price", 0.5).alias("median_price"))
)


df = (
    df.join(median_by_product, on="product_id", how="left")
    .withColumn(
        "price",
        F.when(
            F.col("price") == 0,
            F.coalesce(F.col("median_price"), F.lit(0.0))
        ).otherwise(F.col("price"))
    )
    .drop("median_price")
)


#Xử lý giá trị ngoại lệ
quantiles = (
    df.filter(F.col("price") > 0)
    .approxQuantile("price", [0.01, 0.99], 0.001)
)
price_lower, price_upper = quantiles[0], quantiles[1]


df = df.withColumn(
    "price",
    F.when(F.col("price") < price_lower, price_lower)
    .when(F.col("price") > price_upper, price_upper)
    .otherwise(F.col("price"))
)


#Xử lý giá trị thiếu tại cột user_session
df = df.withColumn(
    "user_session",
    F.when(F.col("user_session").isNull(), F.expr("uuid()"))
    .otherwise(F.col("user_session"))
)


#Chuẩn hóa tất cả dạng NA-string → NULL thực sự
NA_LIKE = ["NA", "None", "none", "null", "NULL", "nan", ""]


def replace_na_strings(df, cols):
    for col in cols:
        df = df.withColumn(
            col,
            F.when(F.col(col).isin(NA_LIKE), None).otherwise(F.col(col))
        )
    return df


df = replace_na_strings(df, ["brand", "cat_0", "cat_1", "cat_2", "cat_3"])


#Fill theo từng cột
df = (
    df
    # brand (9.47%) → "unknown"
    .withColumn("brand", F.coalesce(F.col("brand"), F.lit("unknown")))
    # cat_0, cat_1 (9.71%) → "unknown"
    .withColumn("cat_0", F.coalesce(F.col("cat_0"), F.lit("unknown")))
    .withColumn("cat_1", F.coalesce(F.col("cat_1"), F.lit("unknown")))
    # cat_2 (27.94%) → "unknown"
    .withColumn("cat_2", F.coalesce(F.col("cat_2"), F.lit("unknown")))
    # cat_3 (99.75%, chỉ có 1 giá trị "piano") → DROP
    .drop("cat_3")
)


#Kiểm tra sau khi làm sạch dữ liệu
print("\n=== DỮ LIỆU SAU LÀM SẠCH ===")
df.select(
    [F.count(F.when(F.col(c).isNull(), 1)).alias(c) for c in df.columns]
).show(vertical=True, truncate=False)


df.describe("price").show()
print(f"[FINAL] Tổng rows: {df.count():,}  |  Tổng cột: {len(df.columns)}")

#Ghi lại trên HDFS
df.write.mode("overwrite").parquet("hdfs://master:9000/data/test_cleaned.parquet")
print("Đã ghi file sạch lên HDFS")

df.createOrReplaceTempView("ecommerce_cleaned")

# 6. Dừng phiên làm việc
spark.stop()