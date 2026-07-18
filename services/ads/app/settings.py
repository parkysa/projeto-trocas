from functools import lru_cache

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    ads_host: str = "0.0.0.0"
    ads_port: int = 8002

    ads_db_name: str
    ads_db_user: str
    ads_db_password: str
    ads_db_host: str = "ads-db"
    ads_db_port: int = 5432

    @property
    def database_url(self) -> str:
        return (
            f"postgresql+asyncpg://{self.ads_db_user}:{self.ads_db_password}"
            f"@{self.ads_db_host}:{self.ads_db_port}/{self.ads_db_name}"
        )

    class Config:
        env_file = ".env"
        extra = "ignore"


@lru_cache
def get_settings() -> Settings:
    return Settings()
