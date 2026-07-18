FROM python:3.11-slim

WORKDIR /app

COPY services/ads/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/ads/app ./app

CMD uvicorn app.main:app --host $ADS_HOST --port $ADS_PORT
