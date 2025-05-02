from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType

# Tạo SparkSession với Kafka
spark = SparkSession.builder \
    .appName("KafkaIntegration") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .getOrCreate()

# Đọc dữ liệu từ Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "mobile_topic") \
    .load()

# Định nghĩa schema cho JSON
schema = StructType() \
    .add("Tên sản phẩm", StringType()) \
    .add("Loại điện thoại", StringType())

# Phân tích dữ liệu JSON
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Ghi dữ liệu ra console
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()