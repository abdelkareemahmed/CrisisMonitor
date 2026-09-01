FROM apache/airflow:2.7.1

USER root
RUN apt-get update && \
    apt-get install -y default-jre-headless && \
    apt-get clean

# تعريف مسار الجافا عشان PySpark يلاقيه
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow
RUN pip install pyspark==3.5.0 kafka-python python-dotenv requests