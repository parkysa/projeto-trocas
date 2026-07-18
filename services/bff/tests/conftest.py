import os

os.environ.setdefault("BFF_HOST", "0.0.0.0")
os.environ.setdefault("BFF_PORT", "8000")
os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
os.environ.setdefault("BFF_KAFKA_REPLY_TIMEOUT_SECONDS", "10")
os.environ.setdefault("JWT_SECRET_KEY", "test-secret")
os.environ.setdefault("JWT_ALGORITHM", "HS256")
