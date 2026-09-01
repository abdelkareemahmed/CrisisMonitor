import os
import requests
from dotenv import load_dotenv  # 👈 ضفنا المكتبة دي
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StructType, StructField, BooleanType, FloatType, StringType

# 👈 تحميل المتغيرات المخفية
load_dotenv()

# ==========================================
# 1. إعدادات Azure والـ API
# ==========================================
AZURE_STORAGE_ACCOUNT = "crisismonitordatalake"
AZURE_ACCESS_KEY = os.getenv("AZURE_ACCESS_KEY")  # 👈 قراءة المفتاح من ملف .env بأمان
API_URL = "http://model-api:8000/predict"

# مسارات الداتا في Azure (التلات طبقات) باستخدام abfss
BRONZE_PATH = f"abfss://bronze@{AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net/processed_crisis_news"
SILVER_PATH = f"abfss://silver@{AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net/all_predictions"
GOLD_PATH = f"abfss://gold@{AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net/final_dashboard_data"

# ==========================================
# 2. دالة الـ UDF (نفس اللي إنت عملتها في Databricks بس بالـ severity)
# ==========================================
def analyze_text(text):
    if not text:
        return (False, 0.0, "unknown", 0.0)
    try:
        response = requests.post(API_URL, json={"text": text}, timeout=120)
        if response.status_code == 200:
            data = response.json()
            return (
                data.get("is_crisis", False),
                float(data.get("crisis_probability", 0.0)),
                data.get("severity", "unknown"),
                float(data.get("severity_probability", 0.0))
            )
        else:
            return (False, 0.0, "unknown", 0.0)
    except Exception as e:
        return (False, 0.0, "unknown", 0.0)

# تحديد هيكل البيانات اللي راجع من الـ API (تعديل الـ sentiment لـ severity)
result_schema = StructType([
    StructField("is_crisis", BooleanType(), True),
    StructField("crisis_probability", FloatType(), True),
    StructField("severity", StringType(), True),
    StructField("severity_probability", FloatType(), True)
])

# تحويل الدالة لـ Spark UDF
analyze_udf = udf(analyze_text, result_schema)

# ==========================================
# 3. الوظيفة الأساسية (Spark Job)
# ==========================================
def run_inference_job():
    print("⏳ Starting Spark Inference Job (Bronze -> Silver -> Gold)...")
    
    # بناء الـ Spark Session
    spark = SparkSession.builder \
        .appName("CrisisMonitor_MedallionArchitecture") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.4,com.microsoft.azure:azure-storage:8.6.6") \
        .config(f"fs.azure.account.key.{AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net", AZURE_ACCESS_KEY) \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    try:
        # 1. قراءة الداتا من Bronze (اللي السكريبت الأولاني نضفها)
        print("📥 Reading cleaned data from Bronze layer...")
        bronze_df = spark.read.parquet(BRONZE_PATH)

        if bronze_df.isEmpty():
            print("ℹ️ Bronze layer is empty. Nothing to process.")
            return

        # 2. تطبيق الموديل على عمود النص (Inference)
        print("🧠 Sending data to FastAPI Model (Checking Severity)...")
        processed_df = bronze_df.withColumn("api_result", analyze_udf(col("clean_text"))) \
            .withColumn("is_crisis", col("api_result.is_crisis")) \
            .withColumn("crisis_probability", col("api_result.crisis_probability")) \
            .withColumn("severity", col("api_result.severity")) \
            .withColumn("severity_probability", col("api_result.severity_probability")) \
            .drop("api_result")

        # 3. حفظ *كل النتايج* في Silver (كأرشيف كامل للموديل)
        print("💾 Saving ALL predictions to Silver layer...")
        processed_df.write.mode("append").parquet(SILVER_PATH)

        # 4. فلترة الكوارث الحقيقية فقط للـ Gold
        print("🏆 Filtering real crises for Gold layer...")
        # استخدام فلتر صارم يمنع الـ False Positives
        gold_df = processed_df.filter((col("is_crisis") == True) & (col("crisis_probability") >= 85.0))

        # 5. حفظ النتيجة النهائية في Gold
        if not gold_df.isEmpty():
            print("🚨 Real crisis detected! Saving to Gold layer...")
            gold_df.write.mode("append").parquet(GOLD_PATH)
        else:
            print("✅ No real crises detected in this batch. Gold layer unchanged.")
        
        print("✅ Batch Pipeline Completed Successfully!")

    except Exception as e:
        print(f"❌ Error during Spark Inference: {e}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    run_inference_job()