FROM python:3.11-slim

WORKDIR /app

COPY services/users/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/users/app ./app

CMD uvicorn app.main:app --host $USERS_HOST --port $USERS_PORT
