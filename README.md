# 🚨 CrisisMonitor: End-to-End MLOps & Data Engineering Pipeline

A real-time, scalable data pipeline and MLOps system designed to monitor, classify, and archive global crisis news. 
This project integrates **Data Engineering** (Streaming & Batch) with **MLOps** (Model Serving & Tracking) using a modern tech stack.

## 🏗️ System Architecture

The pipeline follows the **Medallion Architecture** (Bronze, Silver, Gold) integrated with Lambda architecture for real-time and batch processing.

1. **Ingestion:** Real-time news fetched via Currents API and published to **Apache Kafka**.
2. **Processing:** **Apache Spark** (Streaming & Batch) consumes Kafka topics, cleans data, and writes to **Azure Data Lake Gen2**.
3. **Machine Learning:** A fine-tuned Hugging Face transformer model predicts crisis severity.
4. **Model Serving:** Deployed via **FastAPI** with continuous model retraining tracked by **MLflow**.
5. **Dashboard:** Live monitoring interface built with **Streamlit**.

## 🛠️ Tech Stack
* **Data Engineering:** Apache Kafka, Apache Spark (PySpark), Azure Data Lake Storage Gen2 (abfss).
* **MLOps:** MLflow, Transformers (Hugging Face), FastAPI.
* **Orchestration & DevOps:** Apache Airflow, Docker, Docker Compose.
* **Frontend:** Streamlit.

## 📂 Project Structure

```text
CrisisMonitor-MLOps/
├── api/                     # FastAPI model serving and Docker configs
├── data_pipeline/           
│   ├── ingestion/           # Kafka producers fetching external APIs
│   ├── streaming/           # Spark Structured Streaming for live radar
│   └── batch/               # Spark batch jobs (Medallion Architecture)
├── ml_model/                
│   ├── model/               # Current production weights (Ignored in Git)
│   ├── model_v2/            # Newly retrained weights (Ignored in Git)
│   └── notebooks/           # Jupyter notebooks for model experiments
├── airflow/                 # DAGs for scheduling batch jobs
├── dashboard.py             # Streamlit live monitoring dashboard
├── retrain_model.py         # Automated model retraining script
└── docker-compose.yml       # Infrastructure orchestration
