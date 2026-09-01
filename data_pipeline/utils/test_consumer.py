import json
from kafka import KafkaConsumer

def consume_news():
    print("⏳ Connecting to Kafka...")
    try:
        # إعداد الـ Consumer عشان يقرأ من كافكا
        consumer = KafkaConsumer(
            'crisis_news',                     # اسم الطابور (Topic) اللي هنقرأ منه
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',      # عشان يقرأ الداتا من أول رسالة اتبعتت
            value_deserializer=lambda x: json.loads(x.decode('utf-8')) # فك التشفير لـ JSON
        )
        
        print("🎧 Listening to live stream on topic 'crisis_news'...\n" + "="*40)
        
        # اللوب دي بتفضل شغالة ومستنية أي داتا جديدة تدخل الطابور
        for message in consumer:
            news = message.value
            print(f"📥 Received: {news['title']}")
            print(f"📰 Source: {news['source']}")
            print(f"🕒 Published At: {news['published_at']}")
            print("-" * 40)
            
    except Exception as e:
        print(f"❌ Connection Error: {e}")

if __name__ == "__main__":
    consume_news()