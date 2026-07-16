import os

os.environ.setdefault("USERS_HOST", "0.0.0.0")
os.environ.setdefault("USERS_PORT", "8001")
os.environ.setdefault("USERS_DB_HOST", "localhost")
os.environ.setdefault("USERS_DB_PORT", "5432")
os.environ.setdefault("USERS_DB_NAME", "users_db")
os.environ.setdefault("USERS_DB_USER", "postgres")
os.environ.setdefault("USERS_DB_PASSWORD", "postgres")
os.environ.setdefault("JWT_SECRET_KEY", "test-secret")
os.environ.setdefault("JWT_ALGORITHM", "HS256")
os.environ.setdefault("JWT_EXPIRATION_MINUTES", "60")
os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
