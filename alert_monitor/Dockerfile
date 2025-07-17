# alert_monitor/Dockerfile

# Use a modern, more secure base image
FROM python:3.10-slim-bookworm

WORKDIR /app

COPY requirements.txt .

# Upgrade pip and setuptools first, then install requirements
RUN pip install --no-cache-dir --upgrade pip setuptools && \
    pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["uvicorn", "monitor:app", "--host", "0.0.0.0", "--port", "8080"]