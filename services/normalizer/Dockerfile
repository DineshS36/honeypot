FROM python:3.12-slim

WORKDIR /app

COPY services/normalizer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared /app/shared
COPY services/normalizer/main.py /app/main.py

ENV PYTHONUNBUFFERED=1

CMD ["python", "main.py"]
