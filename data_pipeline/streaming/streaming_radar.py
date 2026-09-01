import json
import urllib.request
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lower, regexp_replace, trim
from pyspark.sql.types import StructType, StructField, StringType

# ==========================================
# 1. إعدادات Kafka
# ==========================================
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "live_crisis_data"

kafka_schema = StructType([
    StructField("message_id", StringType(), True),
    StructField("channel_name", StringType(), True),
    StructField("text", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("platform", StringType(), True)
])

# ==========================================
# 2. معالجة الداتا بـ API Batching (الصاروخ 🚀)
# ==========================================
def process_micro_batch(batch_df, batch_id):
    pdf = batch_df.toPandas()
    if pdf.empty:
        return
        
    # بنجمع كل الأخبار اللي في الثانية دي في List واحدة
    texts_to_analyze = pdf['clean_text'].tolist()
    if not texts_to_analyze:
        return
        
    try:
        url = "http://127.0.0.1:8000/predict"
        # 🚀 بنبعت اللستة كلها في Request واحد (API Batching) بدل For Loop
        data = json.dumps({"texts": texts_to_analyze}).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers={'Content-Type': 'application/json'})
        
        with urllib.request.urlopen(req, timeout=120) as response:
            if response.status == 200:
                results = json.loads(response.read().decode('utf-8'))
                
                alarms = []
                # بنطابق النتائج اللي راجعة من الموديل مع الداتا الأصلية
                for i, res in enumerate(results):
                    # سطر لكشف سرعة الموديل في التيرمينال
                    print(f"\n[DEBUG] API Response -> is_crisis: {res['is_crisis']}, prob: {res['crisis_probability']}%, severity: {res['severity']}")
                    
                    # فلتر الإنذار الصارم
                    if res["is_crisis"] and res["crisis_probability"] >= 85.0 and res["severity"] in ["critical", "high"]:
                        alarms.append({
                            "timestamp": pdf.iloc[i].get("timestamp", "NOW"),
                            "platform": pdf.iloc[i].get("platform", "Unknown"),
                            "severity": res["severity"].upper(),
                            "text": pdf.iloc[i]["text"]
                        })
                        
                # طباعة الكوارث وحفظها للـ Dashboard
                if alarms:
                    # 🚀 التعديل الجديد: حفظ الكوارث في ملف عشان Streamlit يقرأه
                    with open("live_alarms.jsonl", "a", encoding="utf-8") as f:
                        for a in alarms:
                            f.write(json.dumps(a) + "\n")

                    print(f"\n🚨 === [NEW DISASTERS DETECTED - BATCH {batch_id}] === 🚨")
                    for a in alarms:
                        print(f"⚠️ [{a['severity']}] | Platform: {a['platform']} | Time: {a['timestamp']}")
                        print(f"📝 {a['text']}")
                        print("-" * 60)
                    print("========================================================\n")
    except Exception as e:
        print(f"\n[ERROR] Connection or Logic Error: {e}")

# ==========================================
# 3. تشغيل الرادار
# ==========================================
def run_radar():
    print("📡 Starting Live Crisis Radar (Streaming using foreachBatch + API Batching)...")
    print("⏳ Waiting for disasters...")
    
    spark = SparkSession.builder \
        .appName("LiveCrisisRadar") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")

    # 1. القراءة من كافكا
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # 2. فك التشفير والتنظيف
    parsed_stream = raw_stream.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), kafka_schema).alias("data")) \
        .select("data.*")

    cleaned_stream = parsed_stream \
        .withColumn("clean_text", lower(col("text"))) \
        .withColumn("clean_text", regexp_replace(col("clean_text"), r"http\S+|www\.\S+", "")) \
        .withColumn("clean_text", regexp_replace(col("clean_text"), r"[^a-z0-9\s]", "")) \
        .withColumn("clean_text", trim(regexp_replace(col("clean_text"), r"\s+", " ")))

    # 3. استخدام foreachBatch
    query = cleaned_stream.writeStream \
        .outputMode("append") \
        .foreachBatch(process_micro_batch) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    run_radar()