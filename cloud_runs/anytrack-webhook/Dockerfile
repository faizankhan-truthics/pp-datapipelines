FROM python:3.12-slim

WORKDIR /srv

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

CMD exec gunicorn --bind 0.0.0.0:$PORT --workers 2 --threads 4 --timeout 30 app.main:app
