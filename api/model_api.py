from fastapi import FastAPI
from pydantic import BaseModel
from typing import List
from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline
import torch

app = FastAPI(title="CrisisMonitor API", description="API to classify disasters and analyze severity (Extreme Performance)")

# --- 1. تحميل موديل الكوارث بتاعك (مع تسريع الأداء) ---
MODEL_PATH = r"../ml_model/model"
print("🔄 جاري تحميل موديل الكوارث اللي إنت دربته...")
crisis_tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH)
crisis_model_fp32 = AutoModelForSequenceClassification.from_pretrained(MODEL_PATH)

# 🚀 التسريع السحري: ضغط الموديل لـ 8-bit عشان يطير على الـ CPU ويشتغل أسرع 4 مرات
print("⚡ جاري ضغط الموديل لتسريع الأداء (INT8 Dynamic Quantization)...")
crisis_model = torch.quantization.quantize_dynamic(
    crisis_model_fp32, {torch.nn.Linear}, dtype=torch.qint8
)

# --- 2. تحميل الفلتر الذكي وموديل الخطورة (النسخة الخفيفة السريعة بدل bart-large) ---
print("🔄 جاري تحميل موديل الفلترة وتحديد الخطورة (النسخة السريعة)...")
severity_analyzer = pipeline(
    "zero-shot-classification", 
    model="cross-encoder/nli-distilroberta-base"
)
print("✅ كل الموديلات جاهزة وبيستقبلوا طلبات دلوقتي!")

# 🚀 تعديل الـ Request عشان يستقبل Batch (لستة أخبار) بدل خبر واحد
class TextBatchRequest(BaseModel):
    texts: List[str]

# ⚡ Asynchronous Endpoint
@app.post("/predict")
async def predict_crisis_batch(request: TextBatchRequest):
    if not request.texts:
        return []
        
    results = []
    
    # --- الجزء الأول: هل دي كارثة؟ (معالجة الباتش كله في ضربة واحدة Vectorization) ---
    inputs = crisis_tokenizer(request.texts, padding=True, truncation=True, return_tensors="pt", max_length=512)
    with torch.no_grad():
        inputs.pop("token_type_ids", None)
        outputs = crisis_model(**inputs)
        predictions = torch.nn.functional.softmax(outputs.logits, dim=-1)
    
    # نلف على النتائج خبر بخبر عشان الفلتر الذكي
    for i, text in enumerate(request.texts):
        crisis_prob = predictions[i][1].item()
        is_crisis = bool(crisis_prob > 0.85)
        
        severity_label = "low"
        severity_score = 0.0

        # --- الجزء التاني: فلتر الحماية وتحديد الخطورة ---
        if is_crisis:
            # 1. فلتر الحماية: نتأكد إنها كارثة كبرى مش جريمة محلية
            verification_labels = ["natural disaster or global crisis", "local police incident or normal crime", "traffic or weather"]
            verification = severity_analyzer(text, verification_labels)
            top_category = verification["labels"][0]
            
            if top_category in ["local police incident or normal crime", "traffic or weather"]:
                # الموديل الذكي اكتشف إن الموديل بتاعك اتخدع في كلمة زي killed أو crash
                is_crisis = False
            else:
                # 2. طالما عدت من الفلتر وطلعت كارثة حقيقية، نحسب الـ Severity
                sev_labels = ["critical", "high", "medium", "low"]
                sev_result = severity_analyzer(text, sev_labels)
                severity_label = sev_result["labels"][0]
                severity_score = sev_result["scores"][0]

        # النتيجة النهائية للخبر ده
        results.append({
            "text": text,
            "is_crisis": is_crisis,
            "crisis_probability": round(crisis_prob * 100, 2),
            "severity": severity_label,
            "severity_probability": round(severity_score * 100, 2)
        })

    return results