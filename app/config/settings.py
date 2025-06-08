# app/config/settings.py
import os

class Settings:
    PROJECT_NAME: str = "Data Forge"
    DEBUG: bool = os.getenv("DEBUG", "False").lower() == "true"
    DATABASE_PATH: str = os.getenv("DATABASE_PATH", "./data/data.duckdb")

    DUCKDB_PERFORMANCE_CONFIG = {
        'memory_limit': '4GB',           # Optimized for local use
        'threads': 'auto',               # Use available cores
        'enable_object_cache': True,
        'temp_directory': '/tmp/duckdb', # SSD-based temp storage
        'checkpoint_threshold': '1GB',   # For write-heavy workloads
        'wal_autocheckpoint': 1000,      # WAL management
    }

settings = Settings()
