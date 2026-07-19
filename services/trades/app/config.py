from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    trades_host: str
    trades_port: int

    trades_db_host: str
    trades_db_port: int
    trades_db_name: str
    trades_db_user: str
    trades_db_password: str

    kafka_bootstrap_servers: str
    trades_kafka_reply_timeout_seconds: int
    kafka_retry_attempts: int
    kafka_retry_delay_seconds: int
    kafka_dlq_topic: str

    @property
    def database_url(self) -> str:
        return (
            f"postgresql+asyncpg://{self.trades_db_user}:{self.trades_db_password}"
            f"@{self.trades_db_host}:{self.trades_db_port}/{self.trades_db_name}"
        )


settings = Settings()
