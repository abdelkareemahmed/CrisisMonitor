FROM python:3.9-slim

# السطر السحري اللي بيمنع بايثون يخزن اللوجز ويطبعها فوراً
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

CMD ["uvicorn", "model_api:app", "--host", "0.0.0.0", "--port", "8000"]