from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    bff_host: str
    bff_port: int

    kafka_bootstrap_servers: str
    bff_kafka_reply_timeout_seconds: int


settings = Settings()
