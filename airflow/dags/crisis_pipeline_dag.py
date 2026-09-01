from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'Kareemello',
    'depends_on_past': False,
    'start_date': datetime(2026, 8, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'crisis_monitor_pipeline',
    default_args=default_args,
    description='Spark Processing & ML Prediction DAG',
    schedule_interval='@hourly',
    catchup=False
) as dag:

    # استخدام BashOperator لتشغيل سبارك كـ Process خارجية منفصلة آمنة
    task_1_processing = BashOperator(
        task_id='clean_data_with_pyspark',
        bash_command='python -u /opt/airflow/project/data_pipeline/batch/kafka_to_bronze.py'
    )

    # 🚀 التعديل هنا: تشغيل سكريبت الموديل الفعلي بدل الـ echo
    task_2_ml_model = BashOperator(
        task_id='predict_crisis_and_sentiment',
        # حط هنا مسار الملف الجديد اللي لسه حافظينه
        bash_command='python -u /opt/airflow/project/data_pipeline/batch/medallion_inference.py' 
    )

    task_1_processing >> task_2_ml_model