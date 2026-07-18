FROM python:3.11-slim

WORKDIR /app

COPY services/bff/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/bff/app ./app

CMD uvicorn app.main:app --host $BFF_HOST --port $BFF_PORT
