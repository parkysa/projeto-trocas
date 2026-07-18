from functools import lru_cache

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    trades_host: str = "0.0.0.0"
    trades_port: int = 8003

    trades_db_name: str
    trades_db_user: str
    trades_db_password: str
    trades_db_host: str = "trades-db"
    trades_db_port: int = 5432

    @property
    def database_url(self) -> str:
        return (
            f"postgresql+asyncpg://{self.trades_db_user}:{self.trades_db_password}"
            f"@{self.trades_db_host}:{self.trades_db_port}/{self.trades_db_name}"
        )

    class Config:
        env_file = ".env"
        extra = "ignore"


@lru_cache
def get_settings() -> Settings:
    return Settings()
