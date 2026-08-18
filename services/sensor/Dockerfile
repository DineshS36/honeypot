FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ../../shared /app/shared
COPY main.py .

ENV PYTHONUNBUFFERED=1

CMD ["python", "main.py"]