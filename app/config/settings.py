# app/config/settings.py
import os
import tempfile
import platform

class Settings:
    PROJECT_NAME: str = "Data Forge"
    DEBUG: bool = os.getenv("DEBUG", "False").lower() == "true"
    DATABASE_PATH: str = os.getenv("DATABASE_PATH", "./data/data.duckdb")

    @property
    def DUCKDB_PERFORMANCE_CONFIG(self):
        # Get platform-appropriate temp directory
        if platform.system() == "Windows":
            temp_dir = os.path.join(tempfile.gettempdir(), "duckdb")
        else:
            temp_dir = "/tmp/duckdb"
        
        # Ensure temp directory exists
        os.makedirs(temp_dir, exist_ok=True)
        
        return {
            'memory_limit': '12GB',          # Optimized for 16GB RAM (75% usage)
            'threads': 12,                   # Use all logical cores (i7 10th Gen)
            'enable_object_cache': True,
            'temp_directory': temp_dir,      # Platform-appropriate temp storage
            'allow_unsigned_extensions': True,  # Allow local extensions
            'autoinstall_known_extensions': False,  # Disable auto-download
            'autoload_known_extensions': False,     # Disable auto-load
            'disabled_optimizers': '',       # Enable all optimizers
            'checkpoint_threshold': '1GB',   # Less frequent checkpoints for performance
            'wal_autocheckpoint': 1000000,   # Reduce WAL checkpoint frequency
        }

settings = Settings()
