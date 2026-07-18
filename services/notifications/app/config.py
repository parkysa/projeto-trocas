from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    notifications_host: str
    notifications_port: int

    notifications_db_host: str
    notifications_db_port: int
    notifications_db_name: str
    notifications_db_user: str
    notifications_db_password: str

    kafka_bootstrap_servers: str
    kafka_retry_attempts: int
    kafka_retry_delay_seconds: int
    kafka_dlq_topic: str

    @property
    def database_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.notifications_db_user}:{self.notifications_db_password}"
            f"@{self.notifications_db_host}:{self.notifications_db_port}/{self.notifications_db_name}"
        )


settings = Settings()
