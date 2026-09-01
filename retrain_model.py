import pandas as pd
import mlflow
import torch
from datasets import Dataset
from transformers import AutoTokenizer, AutoModelForSequenceClassification, Trainer, TrainingArguments

# 1. إعداد MLflow
mlflow.set_tracking_uri("http://127.0.0.1:5000") # ده الرابط اللي MLflow هيشتغل عليه
mlflow.set_experiment("CrisisMonitor_Retraining")

# تفعيل التسجيل الأوتوماتيكي (السحر بتاع MLflow)
mlflow.transformers.autolog()

def load_new_data():
    # هنا بنفترض إنك عندك ملف CSV فيه الداتا الجديدة اللي اتجمعت
    # لو معندكش، اعمل ملف وهمي اسمه new_data.csv فيه عمودين: text و label (0 أو 1)
    try:
        df = pd.read_csv("new_data.csv")
    except FileNotFoundError:
        print("⚠️ ملف new_data.csv مش موجود. هعملك داتا وهمية للتجربة...")
        df = pd.DataFrame({
            "text": ["A massive earthquake just hit the city", "I am drinking coffee in the cafe"],
            "label": [1, 0]
        })
    
    return Dataset.from_pandas(df)

def main():
    print("🔄 جاري سحب البيانات الجديدة...")
    dataset = load_new_data()
    
    # تحميل الموديل بتاعك
    MODEL_PATH = "./ml_model/model"
    print("🔄 جاري تحميل الموديل الحالي...")
    tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH)
    model = AutoModelForSequenceClassification.from_pretrained(MODEL_PATH, num_labels=2)
    
    # تجهيز الداتا للتدريب (Tokenization)
    def tokenize_function(examples):
        return tokenizer(examples["text"], padding="max_length", truncation=True, max_length=128)
    
    tokenized_datasets = dataset.map(tokenize_function, batched=True)
    
    # إعدادات التدريب
    training_args = TrainingArguments(
        output_dir="./results",
        num_train_epochs=3,
        per_device_train_batch_size=8,
        learning_rate=2e-5,
        logging_steps=2,
        report_to="mlflow" # توجيه التقارير لـ MLflow
    )
    
    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=tokenized_datasets,
    )
    
    # 2. بدء عملية التدريب جوه MLflow Run
    with mlflow.start_run() as run:
        print(f"🚀 بدء التدريب المستمر... (Run ID: {run.info.run_id})")
        trainer.train()
        
        # حفظ النسخة الجديدة من الموديل
        new_model_path = "./model_v2"
        trainer.save_model(new_model_path)
        tokenizer.save_pretrained(new_model_path)
        print(f"✅ تم حفظ الموديل الجديد في: {new_model_path}")

if __name__ == "__main__":
    main()