FROM python:3.12-slim

WORKDIR /app

COPY services/sensor/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared /app/shared
COPY services/sensor/main.py /app/main.py

ENV PYTHONUNBUFFERED=1

CMD ["python", "main.py"]