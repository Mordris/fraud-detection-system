# alert_monitor/Dockerfile

FROM python:3.10.13-slim-bullseye

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["uvicorn", "monitor:app", "--host", "0.0.0.0", "--port", "8080"]