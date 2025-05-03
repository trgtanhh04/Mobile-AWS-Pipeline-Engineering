import sys
import os
import logging
from kafka import KafkaConsumer
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, explode, from_json, udf, when,  abs as spark_abs, array, concat_ws
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.types import ArrayType, MapType, IntegerType, FloatType, BooleanType, DoubleType
import re
import json

# Configure logging
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
LOG_DIR = os.path.join(os.path.dirname(__file__), "..", "logs")
os.makedirs(LOG_DIR, exist_ok=True)

# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
#     handlers=[
#         logging.FileHandler(os.path.join(LOG_DIR, "mobile_etl.log"), 'a'),
#         logging.StreamHandler()
#     ]
# )
# logger = logging.getLogger(__name__)

# Cấu hình logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()  # Ghi log ra console
    ]
)

# Tạo logger
logger = logging.getLogger(__name__)

from utils.constants import KAFKA_BOOTSTRAP_SERVERS, TOPIC_PHONE_DATA
from utils.constants import INPUT_PATH, OUTPUT_PATH
# ========== CONECT + EXTRACT DATA FROM KAFKA ==========
def consume_from_kafka():
    """
    Sử dụng KafkaConsumer để kiểm tra kết nối trực tiếp và nhận dữ liệu từ Kafka topic.
    """
    logger.info("Starting Kafka consumer...")
    try:
        # Khởi tạo Kafka consumer
        consumer = KafkaConsumer(
            TOPIC_PHONE_DATA,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='earliest',  # Bắt đầu đọc từ đầu
            enable_auto_commit=True,
            group_id='mobile_group'
        )
        
        logger.info(f"Started consuming messages from Kafka topic: {TOPIC_PHONE_DATA}")
        
        # Lắng nghe và in các tin nhắn nhận được
        for message in consumer:
            logger.info(f"Received message: {message.value.decode('utf-8')}")
            print(f"Message: {message.value.decode('utf-8')}")
    
    except Exception as e:
        logger.error(f"Error consuming from Kafka: {e}")
        raise e

def init_spark_session():
    """
    Khởi tạo Spark session.
    """
    logger.info("Initializing Spark session...")
    try:
        spark = SparkSession.builder \
            .appName("MobileETL") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/spark_checkpoints") \
            .getOrCreate()
                    # .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
        logger.info("Spark session initialized.")
        return spark
    except Exception as e:
        logger.error(f"Failed to initialize Spark session: {e}")
        raise e
    
def connect_to_kafka(spark):
    """
    Kết nối đến Kafka và nhận dữ liệu từ topic.
    """
    logger.info("Connecting to Kafka...")
    try:
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", 'kafka:9092') \
            .option("subscribe", "mobile_topic") \
            .option("startingOffsets", "latest") \
            .load()
        
        # json_output_path = f"{OUTPUT_PATH}/parsed_json_output"
        # logger.info(f"Saving parsed data to JSON at {json_output_path}.")
        # df_tmp = df.writeStream \
        #     .format("json") \
        #     .outputMode("append") \
        #     .option("path", json_output_path) \
        #     .option("checkpointLocation", "/tmp/spark_checkpoints/json_checkpoint") \
        #     .start()
        
        # df_tmp.awaitTermination(5)  # Chờ 5 giây để ghi dữ liệu vào file JSON

        # print(df.head())
        df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        logger.info("Connected to Kafka topic.")
        return df

    except Exception as e:
        logger.error(f"Failed to connect to Kafka: {e}")
        raise e

def parse_kafka_data(df):
    """
    Parse dữ liệu Kafka thành các cột đúng định dạng.
    """
    logger.info("Parsing Kafka data...")
    schema = StructType([
        StructField("Tên sản phẩm", StringType()),
        StructField("Loại điện thoại", StringType()),
        StructField("Màu sắc - Phiên bản bộ nhớ - Giá tương ứng", ArrayType(
            ArrayType(StringType())  # Đây là danh sách các danh sách nhỏ [Màu sắc, Phiên bản, Giá, Giá cũ]
        )),
        StructField("Thời gian bảo hành", StringType()),
        StructField("Thông số kỹ thuật", StringType()),  # Thông số kỹ thuật hiện là JSON dạng String
        StructField("Đánh giá", StringType()),
        StructField("Số lượt đánh giá và hỏi đáp", StringType()),
        StructField("Đường dẫn", StringType())
    ])
    try:
        parsed_df = df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), schema).alias("data")) \
            .select("data.*")

        logger.info("Parsed Kafka data.")
        return parsed_df
    except Exception as e:
        logger.error(f"Failed to parse Kafka data: {e}")
        raise e
# ========== TRANSFORM + LOAD DATA ==========


# Định nghĩa hàm get_brand
def get_brand(s):
    if not isinstance(s, str):
        return None
    s = s.replace("Điện thoại", "")
    s = s.strip()
    res = s.split(" ")[0].strip()
    if res == "OPPO":
        return "Oppo"
    if "Bkav" in res:
        return "Bphone"
    if res == "Thành":
        return "Thành Hưng"
    return res

# Định nghĩa hàm isOld
def isOld(s):
    if not isinstance(s, str):
        return None
    s = s.lower()
    return "cũ" in s

# Định nghĩa hàm cast_price
def cast_price(s):
    if not isinstance(s, str):
        return None  # Tương đương với np.nan trong Pandas
    s = s.replace(".", "")
    numbers = re.findall(r'\d+', s)
    if len(numbers) == 0:
        return None  # Tương đương với np.nan trong Pandas
    return int(numbers[0])

# Định nghĩa hàm cast_warranty
def cast_warranty(s):
    if not isinstance(s, str):
        return None  # Tương đương với np.nan trong Pandas
    numbers = re.findall(r'\d+', s)
    if len(numbers) == 0:
        return None  # Tương đương với np.nan trong Pandas
    if len(numbers) == 1:
        if numbers[0] == "30":
            return 1
        return int(numbers[0])
    if numbers[0] == "6" and numbers[1] == "12":
        return 12
    if numbers[0] == "12" and numbers[1] == "12":
        return 24
    return int(numbers[0])


def cast_rating(s):
    if not isinstance(s, str):
        return None  # Tương đương với np.nan trong Pandas
    numbers = re.findall(r"-?\d+(?:\.\d+)?", s)
    if len(numbers) == 0:
        return None  # Tương đương với np.nan trong Pandas
    return float(numbers[0])

# Định nghĩa hàm cast_memory_version
def cast_memory_version(s):
    if not isinstance(s, str):
        return [None, None]  # Tương đương với [np.nan, np.nan]
    numbers = re.findall(r"-?\d+(?:\.\d+)?", s)
    if len(numbers) == 0:
        return [None, None]
    if len(numbers) == 1:
        if abs(float(numbers[0])) < 4:
            return [None, abs(float(numbers[0]) * 1024)]
        return [None, abs(float(numbers[0]))]
    if abs(float(numbers[1])) < 4:
        return [abs(float(numbers[0])), abs(float(numbers[1]) * 1024)]
    return [abs(float(numbers[0])), abs(float(numbers[1]))]

# Hàm xử lý RAM
def firstNumberRAM(s):
    if not isinstance(s, str):
        return None  # Tương đương với np.nan trong Pandas
    numbers = re.findall(r"-?\d+(?:\.\d+)?", s)
    if len(numbers) == 0:
        return None
    res = float(numbers[0])
    return res if res < 100 else res * 1.0 / 1024

# Hàm xử lý ROM
def firstNumberROM(s):
    if not isinstance(s, str):
        return None  # Tương đương với np.nan trong Pandas
    numbers = re.findall(r"-?\d+(?:\.\d+)?", s)
    if len(numbers) == 0:
        return None
    res = float(numbers[0])
    if res < 2:
        res = res * 1024
    return res


# Hàm trích xuất kích thước màn hình (inches)
def extractInches(s):
    if not isinstance(s, str):
        return None
    s = s.lower()
    match = re.search(r"(\d+[.,]?\d*)\s*inches", s)
    result = float(match.group(1).replace(",", ".")) if match else None
    return result

# Hàm trích xuất tần số quét (Hz)
def extractHz(s):
    if not isinstance(s, str):
        return None
    s = s.lower()
    match = re.search(r"(\d+(\.\d+)?)\s*hz", s)
    result = float(match.group(1)) if match else None
    return result

# Hàm trích xuất độ sáng màn hình (nits)
def extractNits(s):
    if not isinstance(s, str):
        return None
    s = s.lower()
    match = re.search(r"(\d+(\.\d+)?)\s*nits", s)
    result = float(match.group(1)) if match else None
    if result and result < 50:
        result = result * 1000
    return result

# Hàm trích xuất loại màn hình
def extractType(s):
    if not isinstance(s, str):
        return None
    s = s.upper()
    if "AMOLED" in s:
        return "AMOLED"
    if "OLED" in s:
        return "OLED"
    if "LCD" in s:
        return "LCD"
    return None


# Định nghĩa hàm extractSim
def extractSim(s):
    if not isinstance(s, str):
        return None  # Tương đương với np.nan trong Pandas
    s = s.lower()
    if "2" in s or "dual" in s or "kép" in s:
        return 2
    if "không" in s:
        return 0
    return 1


# Hàm trích xuất loại pin
def batteryType(s):
    if not isinstance(s, str):
        return ""
    s = s.lower()
    if "li-po" in s:
        return "Li-Po"
    if "li-ion" in s:
        return "Li-Ion"
    if "si/c" in s:
        return "Si/C"
    return ""

# Hàm trích xuất dung lượng pin
def batteryCapacity(s):
    if not isinstance(s, str):
        return None  # Tương đương với np.nan trong Pandas
    s = s.lower()
    match = re.search(r"(\d+(\.\d+)?)\s*mah", s)
    result = float(match.group(1)) if match else None
    return result

# Hàm trích xuất công suất sạc
def chargingCapacity(s):
    if not isinstance(s, str):
        return None  # Tương đương với np.nan trong Pandas
    s = s.lower()
    match = re.search(r"(\d+(\.\d+)?)\s*w", s)
    result = float(match.group(1)) if match else None
    return result



# Hàm trích xuất độ phân giải camera
def extractMP(s):
    if not isinstance(s, str):
        return []  # Tương đương với danh sách trống
    s = s.upper()
    matches = re.findall(r"(\d+(\.\d+)?)\s*MP", s)
    result = [float(match[0]) for match in matches]
    return result


# Hàm chuyển đổi hệ điều hành
def cast_OS(s):
    if not isinstance(s, str):
        return "Khác"
    s = s.lower()
    numbers = re.findall(r"\d+", s)
    if len(numbers) == 0:
        return "Khác"
    if "android" in s:
        return "Android " + str(int(numbers[0]))
    if "ios" in s:
        return "iOS " + str(int(numbers[0]))
    return "Khác"

def transform_data(parsed_df):
    """
    Chuyển đổi dữ liệu từ DataFrame.
    """
    logger.info("Transforming data...")
    try:
        renamed_columns = {
            "Tên sản phẩm": "ten",
            "Loại điện thoại": "loai_dien_thoai",
            "Màu sắc - Phiên bản bộ nhớ - Giá tương ứng": "mau_sac_phien_ban_bo_nho_gia_moi_gia_cu",
            "Thời gian bảo hành": "thoi_gian_bao_hanh",
            "Thông số kỹ thuật": "thong_so_ky_thuat",
            "Đánh giá": "danh_gia",
            "Số lượt đánh giá và hỏi đáp": "so_luong_binh_luan",
            "Đường dẫn": "duong_dan"
        }

        for old_name, new_name in renamed_columns.items():
            parsed_df = parsed_df.withColumnRenamed(old_name, new_name)
        
        # Loại bỏ các hàng có giá trị "ten" không mong muốn
        parsed_df = parsed_df.filter(col("ten") != "Điện thoại Xiaomi SU7 (Ô tô điện chạy 800km/1 lần sạc)")
        parsed_df = parsed_df.filter((col("loai_dien_thoai") != "Apple Watch") & 
                                    (col("loai_dien_thoai") != "Máy Chơi Game PC"))
        
        # Thuộc tính: Màu sắc, Phiên bản bộ nhớ, Giá mới, Giá cũ
        # parsed_df = parsed_df.withColumn(
        #     "mau_sac_phien_ban_bo_nho_gia_moi_gia_cu",
        #     F.from_json(
        #         F.col("mau_sac_phien_ban_bo_nho_gia_moi_gia_cu"),
        #         ArrayType(ArrayType(StringType())) 
        #     )
        # )

        # parsed_df = parsed_df.withColumn(
        #     "exploded",
        #     F.explode("mau_sac_phien_ban_bo_nho_gia_moi_gia_cu")
        # )
        # parsed_df = parsed_df.withColumn("mau_sac", F.col("exploded")[0]) \
        #                     .withColumn("phien_ban_bo_nho", F.col("exploded")[1]) \
        #                     .withColumn("gia_moi", F.col("exploded")[2]) \
        #                     .withColumn("gia_cu", F.col("exploded")[3]) \
        #                     .drop("exploded", "mau_sac_phien_ban_bo_nho_gia_moi_gia_cu")

        parsed_df = parsed_df.withColumn(
            "mau_sac_phien_ban_bo_nho_gia_moi_gia_cu",
            F.from_json(
                F.to_json(F.col("mau_sac_phien_ban_bo_nho_gia_moi_gia_cu")),  # <<< CHỈ THÊM to_json()
                ArrayType(ArrayType(StringType()))
            )
        )

        parsed_df = parsed_df.withColumn(
            "exploded",
            F.explode("mau_sac_phien_ban_bo_nho_gia_moi_gia_cu")
        )

        parsed_df = parsed_df.withColumn("mau_sac", F.col("exploded")[0]) \
                            .withColumn("phien_ban_bo_nho", F.col("exploded")[1]) \
                            .withColumn("gia_moi", F.col("exploded")[2]) \
                            .withColumn("gia_cu", F.col("exploded")[3]) \
                            .drop("exploded", "mau_sac_phien_ban_bo_nho_gia_moi_gia_cu")


        # Thuộc tính: Màn hình, Hệ điều hành, Camera sau, Camera trước, CPU, RAM, Bộ nhớ trong, Thẻ SIM, Dung lượng pin, Thiết kế
        parsed_df = parsed_df.withColumn(
            "thong_so_ky_thuat",
            from_json(col("thong_so_ky_thuat"), MapType(StringType(), StringType()))
        )
        parsed_df = parsed_df.withColumn("man_hinh",        col("thong_so_ky_thuat")["Màn hình:"]) \
                            .withColumn("he_dieu_hanh",     col("thong_so_ky_thuat")["Hệ điều hành:"]) \
                            .withColumn("camera_sau",       col("thong_so_ky_thuat")["Camera sau:"]) \
                            .withColumn("camera_truoc",     col("thong_so_ky_thuat")["Camera trước:"]) \
                            .withColumn("cpu",              col("thong_so_ky_thuat")["CPU:"]) \
                            .withColumn("ram",              col("thong_so_ky_thuat")["RAM:"]) \
                            .withColumn("bo_nho_trong",     col("thong_so_ky_thuat")["Bộ nhớ trong:"]) \
                            .withColumn("the_sim",          col("thong_so_ky_thuat")["Thẻ SIM:"]) \
                            .withColumn("dung_luong_pin",   col("thong_so_ky_thuat")["Dung lượng pin:"]) \
                            .withColumn("thiet_ke",         col("thong_so_ky_thuat")["Thiết kế:"])
        parsed_df = parsed_df.drop("thong_so_ky_thuat")

        # Thuộc tính: Hãng điện thoại
        get_brand_udf = udf(get_brand, StringType())
        parsed_df = parsed_df.withColumn("hang_dien_thoai", get_brand_udf(col("ten")))

        # Thuộc tính: Có phải là điện thoại cũ hay không?
        isOld_udf = udf(isOld, BooleanType())

        parsed_df = parsed_df.withColumn("old1", isOld_udf(col("ten")))
        parsed_df = parsed_df.withColumn("old2", isOld_udf(col("loai_dien_thoai")))

        parsed_df = parsed_df.withColumn("la_dien_thoai_cu", col("old1") | col("old2"))

        parsed_df = parsed_df.drop("old1", "old2")


        # Thuộc tính: Giá mới, Giá cũ
        cast_price_udf = udf(cast_price, IntegerType())

        parsed_df = parsed_df.withColumn("gia_moi", cast_price_udf(col("gia_moi")))
        parsed_df = parsed_df.withColumn("gia_cu", cast_price_udf(col("gia_cu")))

        # Thuộc tính: Thời gian bảo hành
        cast_warranty_udf = udf(cast_warranty, IntegerType())
        parsed_df = parsed_df.withColumn("thoi_gian_bao_hanh", cast_warranty_udf(col("thoi_gian_bao_hanh")))


        # Thuộc tính: Đánh giá
        cast_rating_udf = udf(cast_rating, FloatType())
        parsed_df = parsed_df.withColumn("danh_gia", cast_rating_udf(col("danh_gia")))

        # Thuộc tính: Phiên bản bộ nhớ
        cast_memory_version_udf = udf(cast_memory_version, ArrayType(DoubleType()))

        parsed_df = parsed_df.withColumn("phien_ban_bo_nho", cast_memory_version_udf(col("phien_ban_bo_nho")))

        parsed_df = parsed_df.withColumn("_ram", col("phien_ban_bo_nho")[0])
        parsed_df = parsed_df.withColumn("_bo_nho_trong", col("phien_ban_bo_nho")[1])
        parsed_df = parsed_df.drop("phien_ban_bo_nho")


        # Thuộc tính: RAM, Bộ nhớ trong
        firstNumberRAM_udf = udf(firstNumberRAM, DoubleType())
        firstNumberROM_udf = udf(firstNumberROM, DoubleType())

        parsed_df = parsed_df.withColumn("ram", firstNumberRAM_udf(col("ram")))
        parsed_df = parsed_df.withColumn("bo_nho_trong", firstNumberROM_udf(col("bo_nho_trong")))

        parsed_df = parsed_df.withColumn("ram", when(col("ram").isNull(), col("_ram")).otherwise(col("ram")))
        parsed_df = parsed_df.withColumn("bo_nho_trong", when(col("bo_nho_trong").isNull(), col("_bo_nho_trong")).otherwise(col("bo_nho_trong")))

        parsed_df = parsed_df.drop("_ram", "_bo_nho_trong")

        # Thuộc tính: Kích thước màn hình, Tần số quét, Độ sáng màn hình, Loại màn hình
        extractInches_udf = udf(extractInches, DoubleType())
        extractHz_udf = udf(extractHz, DoubleType())
        extractNits_udf = udf(extractNits, DoubleType())
        extractType_udf = udf(extractType, StringType())

        parsed_df = parsed_df.withColumn("kich_thuoc_man_hinh", extractInches_udf(col("man_hinh")))
        parsed_df = parsed_df.withColumn("tan_so_quet", extractHz_udf(col("man_hinh")))
        parsed_df = parsed_df.withColumn("do_sang_man_hinh", extractNits_udf(col("man_hinh")))
        parsed_df = parsed_df.withColumn("loai_man_hinh", extractType_udf(col("man_hinh")))

        parsed_df = parsed_df.drop("man_hinh")

        # Thuộc tính: Số thẻ SIM
        extractSim_udf = udf(extractSim, IntegerType())
        parsed_df = parsed_df.withColumn("so_the_sim", extractSim_udf(col("the_sim")))
        parsed_df = parsed_df.drop("the_sim")

        # Thuộc tính: Loại pin, Dung lượng pin, Công suất sạc
        batteryType_udf = udf(batteryType, StringType())
        batteryCapacity_udf = udf(batteryCapacity, DoubleType())
        chargingCapacity_udf = udf(chargingCapacity, DoubleType())

        parsed_df = parsed_df.withColumn("pin", col("dung_luong_pin"))

        parsed_df = parsed_df.withColumn("loai_pin", batteryType_udf(col("pin")))
        parsed_df = parsed_df.withColumn("dung_luong_pin", batteryCapacity_udf(col("pin")))
        parsed_df = parsed_df.withColumn("cong_suat_sac", chargingCapacity_udf(col("pin")))

        parsed_df = parsed_df.drop("pin")

        # Thuộc tính: Độ phân giải camera trước, Độ phân giải camera sau
        extractMP_udf = udf(extractMP, ArrayType(DoubleType()))

        parsed_df = parsed_df.withColumn("do_phan_giai_cam_sau", extractMP_udf(col("camera_sau")))
        parsed_df = parsed_df.withColumn("do_phan_giai_cam_truoc", extractMP_udf(col("camera_truoc")))

        parsed_df = parsed_df.withColumn("do_phan_giai_cam_sau", concat_ws(",", col("do_phan_giai_cam_sau")))
        parsed_df = parsed_df.withColumn("do_phan_giai_cam_truoc", concat_ws(",", col("do_phan_giai_cam_truoc")))

        parsed_df = parsed_df.drop("camera_sau", "camera_truoc")

        # Thuộc tính: Hệ điều hành
        cast_OS_udf = udf(cast_OS, StringType())
        parsed_df = parsed_df.withColumn("he_dieu_hanh", cast_OS_udf(col("he_dieu_hanh")))

        # Xử lý các cột có giá trị null
        parsed_df = parsed_df.withColumn("thoi_gian_bao_hanh", when(col("thoi_gian_bao_hanh").isNull(), 0).otherwise(col("thoi_gian_bao_hanh")))
        parsed_df = parsed_df.withColumn("mau_sac", when(col("mau_sac") == "", None).otherwise(col("mau_sac")))

        return parsed_df
    except Exception as e:
        logger.error(f"Failed to transform data: {e}")
        raise e
        
# ========== MAIN FUNCTION ==========

all_batches = []

def write_to_csv(batch_df, batch_id):
    global all_batches

    if batch_df.count() == 0:
        logger.warning(f"No data in batch {batch_id} to write.")
    else:
        logger.info(f"Processing batch {batch_id}.")
        # Thu thập dữ liệu từ batch hiện tại
        all_batches.append(batch_df)

        # Gộp tất cả batch lại thành một DataFrame
        combined_df = all_batches[0]
        for df in all_batches[1:]:
            combined_df = combined_df.union(df)

        # Ghi dữ liệu ra file duy nhất
        output_path = f"{OUTPUT_PATH}/kafka_output_combined.csv"
        logger.info(f"Writing combined data to {output_path}.")
        combined_df.write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(output_path)


if __name__ == "__main__":
    # Khởi tạo Spark session
    try:
        logger.info(f"KAFKA_BOOTSTRAP_SERVERS: {KAFKA_BOOTSTRAP_SERVERS}")
        logger.info(f"Kafka topic: {TOPIC_PHONE_DATA}")

        # Tạo Spark session
        spark = SparkSession.builder \
            .appName("MobileETL") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/spark_checkpoints") \
            .getOrCreate()
            # .config("spark.jars", "/opt/airflow/jars/spark-sql-kafka-0-10_2.12-3.1.2.jar,/opt/airflow/jars/kafka-clients-2.8.0.jar") \

        # Kết nối Kafka
        kafka_df = connect_to_kafka(spark)

        parsed_df = parse_kafka_data(kafka_df)

        parsed_df = transform_data(parsed_df)

        # Ghi dữ liệu ra file CSV
        output_path = f"{OUTPUT_PATH}/kafka_output"

        # def write_to_csv(batch_df, batch_id):
        #     if batch_df.count() == 0:
        #         logger.warning("No data in this batch to write.")
        #     else:
        #         logger.info(f"Writing batch {batch_id} to CSV.")
        #         batch_df.write \
        #             .mode("append") \
        #             .option("header", "true") \
        #             .csv(output_path)


        query = parsed_df.writeStream \
            .foreachBatch(write_to_csv) \
            .start()

        # query.awaitTermination()
        import time
        time.sleep(180)
        query.stop()

    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise e