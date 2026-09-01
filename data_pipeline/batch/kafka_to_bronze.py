import os
from dotenv import load_dotenv  # 👈 ضفنا المكتبة دي
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, lower, regexp_replace, trim
from pyspark.sql.types import StructType, StructField, StringType

# 👈 تحميل المتغيرات المخفية
load_dotenv()

# ==========================================
# 1. إعدادات الوصول السحابي (Azure Config)
# ==========================================
AZURE_STORAGE_ACCOUNT = "crisismonitordatalake"
AZURE_ACCESS_KEY = os.getenv("AZURE_ACCESS_KEY")  # 👈 قراءة المفتاح من ملف .env بأمان
CONTAINER_NAME = "bronze"
KAFKA_BROKER = "kafka:29092" # مهم جداً لو Spark شغال جوه دوكر
KAFKA_TOPIC = "live_crisis_data"

# ==========================================
# 2. تعريف هيكل البيانات (Schema) الجديد بناءً على Currents API
# ==========================================
schema = StructType([
    StructField("message_id", StringType(), True),
    StructField("channel_name", StringType(), True),
    StructField("text", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("platform", StringType(), True)
])

def run_spark_job():
    """
    الدالة الرئيسية اللي Airflow هيناديها عشان تشتغل كـ Batch Job
    """
    print("⏳ Starting Spark Batch Job (Kafka to Bronze)...")
    
    # بناء الـ Spark Session باستخدام بروتوكول Gen2 (dfs)
    spark = SparkSession.builder \
        .appName("CrisisNewsBatchProcessor") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-azure:3.3.4,com.microsoft.azure:azure-storage:8.6.6") \
        .config(f"fs.azure.account.key.{AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net", AZURE_ACCESS_KEY) \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN") 
    print("✅ Connected to Spark and Azure Successfully!")

    # ==========================================
    # 4. سحب البيانات كـ Batch (مش Stream)
    # ==========================================
    try:
        raw_df = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKER) \
            .option("subscribe", KAFKA_TOPIC) \
            .load()
            
        # لو كافكا فاضي مفيش داتا جديدة، نقفل باحترام
        if raw_df.isEmpty():
            print("ℹ️ No new data in Kafka. Spark job finishing gracefully.")
            spark.stop()
            return

    except Exception as e:
        print(f"❌ Failed to read from Kafka: {e}")
        spark.stop()
        raise e

    # ==========================================
    # 5. تنظيف البيانات (Data Cleaning للغة الإنجليزية)
    # ==========================================
    print("⚙️ Cleaning the data...")
    parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    processed_df = parsed_df.withColumn("spark_processing_time", current_timestamp())

    # تنظيف النص الإنجليزي: تحويل لحروف صغيرة، إزالة الروابط، إزالة الرموز الخاصة
    cleaned_df = processed_df \
        .withColumn("clean_text", lower(col("text"))) \
        .withColumn("clean_text", regexp_replace(col("clean_text"), r"http\S+|www\.\S+", "")) \
        .withColumn("clean_text", regexp_replace(col("clean_text"), r"[^a-z0-9\s]", "")) \
        .withColumn("clean_text", trim(regexp_replace(col("clean_text"), r"\s+", " ")))

    # ==========================================
    # 6. التخزين في Azure (بصيغة Parquet) باستخدام بروتوكول abfss
    # ==========================================
    print(f"🚀 Writing cleaned batch data to Azure Data Lake ({CONTAINER_NAME})...")
    azure_path = f"abfss://{CONTAINER_NAME}@{AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net/processed_crisis_news"

    try:
        cleaned_df.write \
            .mode("append") \
            .parquet(azure_path)
        print("✅ Data successfully saved to Azure Bronze layer!")
    except Exception as e:
        print(f"❌ Failed to write to Azure: {e}")
        raise e  # ضروري عشان Airflow ينور أحمر لو فيه مشكلة
    finally:
        spark.stop()
        print("🏁 Spark Job Finished.")

if __name__ == "__main__":
    run_spark_job()