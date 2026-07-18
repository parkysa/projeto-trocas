from functools import lru_cache

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    users_host: str = "0.0.0.0"
    users_port: int = 8001

    users_db_name: str
    users_db_user: str
    users_db_password: str
    users_db_host: str = "users-db"
    users_db_port: int = 5432

    @property
    def database_url(self) -> str:
        return (
            f"postgresql+asyncpg://{self.users_db_user}:{self.users_db_password}"
            f"@{self.users_db_host}:{self.users_db_port}/{self.users_db_name}"
        )

    class Config:
        env_file = ".env"
        extra = "ignore"


@lru_cache
def get_settings() -> Settings:
    return Settings()
