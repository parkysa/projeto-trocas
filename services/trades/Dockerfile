FROM python:3.11-slim

WORKDIR /app

COPY services/trades/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/trades/app ./app

CMD uvicorn app.main:app --host $TRADES_HOST --port $TRADES_PORT
