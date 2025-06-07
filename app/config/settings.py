# app/config/settings.py
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    """
    Application settings loaded from environment variables or a .env file.
    """
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    APP_NAME: str = "Data Forge - Dynamic Data Platform"
    DEBUG_MODE: bool = True
    DUCKDB_PATH: str = "data/database.duckdb" # Path to our DuckDB file

settings = Settings()
