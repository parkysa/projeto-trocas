from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    users_host: str
    users_port: int

    users_db_host: str
    users_db_port: int
    users_db_name: str
    users_db_user: str
    users_db_password: str

    jwt_secret_key: str
    jwt_algorithm: str
    jwt_expiration_minutes: int

    kafka_bootstrap_servers: str
    kafka_retry_attempts: int
    kafka_retry_delay_seconds: int
    kafka_dlq_topic: str

    @property
    def database_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.users_db_user}:{self.users_db_password}"
            f"@{self.users_db_host}:{self.users_db_port}/{self.users_db_name}"
        )


settings = Settings()
