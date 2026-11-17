# Container for the ClickUp embed proxy (Cloud Run).
FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY . ./

# Cloud Run provides $PORT; gunicorn binds to it. --timeout 0 so large report downloads aren't cut off.
ENV PORT=8080
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app
