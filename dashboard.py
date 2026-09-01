import streamlit as st
import pandas as pd
import json
import time
import os

# إعدادات الصفحة
st.set_page_config(page_title="CrisisMonitor Dashboard", page_icon="🚨", layout="wide")
st.title("🚨 Live Crisis Monitor Dashboard")
st.markdown("Real-time disaster detection powered by Kafka, Spark, and Hugging Face.")

# حل مشكلة المسار: هندور على الملف سواء إنت مشغل من بره أو من جوه
FILE_PATH = "data_pipeline/streaming/live_alarms.jsonl"
if not os.path.exists(FILE_PATH):
    FILE_PATH = "live_alarms.jsonl"

# دالة لقراءة الكوارث من الملف
def load_alarms():
    alarms = []
    try:
        with open(FILE_PATH, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    alarms.append(json.loads(line))
    except FileNotFoundError:
        pass
    return alarms

alarms_data = load_alarms()

if not alarms_data:
    st.info("🟢 No critical disasters detected currently. Listening to real-time streams...")
else:
    df = pd.DataFrame(alarms_data)
    
    # 1. قسم الإحصائيات السريعة
    total_disasters = len(df)
    critical_count = len(df[df['severity'] == 'CRITICAL'])
    high_count = len(df[df['severity'] == 'HIGH'])
    
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Disasters", total_disasters)
    col2.metric("CRITICAL Alerts 🚨", critical_count)
    col3.metric("HIGH Alerts ⚠️", high_count)
    
    st.markdown("---")
    
    # 2. عرض أحدث الكوارث في شكل جدول شيك
    st.subheader("📝 Latest Disaster Reports")
    # بنعكس الترتيب عشان أحدث حاجة تظهر فوق
    df_display = df.iloc[::-1].reset_index(drop=True)
    
    # تلوين السطور حسب الخطورة
    def color_severity(val):
        color = '#ff4b4b' if val == 'CRITICAL' else '#ffa500' if val == 'HIGH' else 'white'
        return f'color: {color}; font-weight: bold'
        
    st.dataframe(
        df_display.style.map(color_severity, subset=['severity']), 
        use_container_width=True,
        height=400
    )

# الطريقة الرسمية في Streamlit لعمل Refresh للصفحة كل ثانيتين
time.sleep(2)
st.rerun()