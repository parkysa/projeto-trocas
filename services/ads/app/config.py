from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    ads_host: str
    ads_port: int

    ads_db_host: str
    ads_db_port: int
    ads_db_name: str
    ads_db_user: str
    ads_db_password: str

    kafka_bootstrap_servers: str

    @property
    def database_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.ads_db_user}:{self.ads_db_password}"
            f"@{self.ads_db_host}:{self.ads_db_port}/{self.ads_db_name}"
        )


settings = Settings()
