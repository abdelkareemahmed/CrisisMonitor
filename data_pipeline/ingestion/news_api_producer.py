import os
import json
import time
import requests
from dotenv import load_dotenv
from kafka import KafkaProducer

# 1. تحميل الإعدادات
load_dotenv()
API_KEY = os.getenv('CURRENTS_API_KEY')
KAFKA_BROKER = 'localhost:9092' 
KAFKA_TOPIC = 'live_crisis_data'

# 2. إعداد الـ Kafka Producer
try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        api_version=(2, 8, 1),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
    )
    print("✅ تم الاتصال بـ Kafka بنجاح!")
except Exception as e:
    print(f"❌ فشل الاتصال بـ Kafka: {e}")
    exit(1)

# 3. دالة سحب الأخبار من Currents API
def fetch_news():
    url = "https://api.currentsapi.services/v1/latest-news"
    params = {
        "language": "en", 
        "apiKey": API_KEY
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json().get('news', [])
    except Exception as e:
        print(f"⚠️ مشكلة في سحب الأخبار من الـ API: {e}")
        return []

# 4. تشغيل السحب والضخ المستمر
if __name__ == '__main__':
    print("📡 جاري تشغيل الـ News API Producer...")
    
    seen_articles = set()

    while True:
        print("\n⏳ جاري البحث عن أخبار جديدة...")
        news_articles = fetch_news()
        
        for article in news_articles:
            article_id = article.get('id')
            
            if article_id not in seen_articles:
                seen_articles.add(article_id)
                
                data_payload = {
                    'message_id': article_id,
                    'channel_name': article.get('author') or 'وكالة أنباء',
                    'text': f"{article.get('title')} - {article.get('description')}",
                    'timestamp': article.get('published'),
                    'platform': 'CurrentsAPI'
                }
                
                try:
                    producer.send(KAFKA_TOPIC, value=data_payload)
                    producer.flush() # السطر ده بيضمن إن الداتا تتبعت فوراً لكافكا
                    print(f"🚀 تم الضخ -> [{data_payload['channel_name']}]: {data_payload['text'][:60]}...")
                except Exception as e:
                    print(f"⚠️ مشكلة في إرسال الداتا لـ Kafka: {e}")
        
        print("💤 هنام 10 دقايق واستنى الأخبار الجاية...")
        time.sleep(600)