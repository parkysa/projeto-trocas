FROM python:3.11-slim

WORKDIR /app

COPY services/notifications/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/notifications/app ./app

CMD uvicorn app.main:app --host $NOTIFICATIONS_HOST --port $NOTIFICATIONS_PORT
