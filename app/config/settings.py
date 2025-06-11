# app/config/settings.py
import os
import tempfile
import platform

class Settings:
    PROJECT_NAME: str = "Data Forge"
    DEBUG: bool = os.getenv("DEBUG", "False").lower() == "true"
    DATABASE_PATH: str = os.getenv("DATABASE_PATH", "./data/data.duckdb")
    
    # Server settings
    HOST: str = os.getenv("HOST", "0.0.0.0")
    PORT: int = int(os.getenv("PORT", "8080"))
    API_BASE_URL: str = os.getenv("API_BASE_URL", f"http://localhost:{PORT}/api/v1")
    
    # Display settings
    DISPLAY_MAX_ROWS: int = int(os.getenv("DISPLAY_MAX_ROWS", "500"))
    DISPLAY_WIDTH: int = int(os.getenv("DISPLAY_WIDTH", "200"))

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
            'memory_limit': '12GB',           # Optimized for local use
            'threads': 4,                    # Fixed number for stability
            'enable_object_cache': True,
            'temp_directory': temp_dir,      # Platform-appropriate temp storage
            'allow_unsigned_extensions': True,  # Allow local extensions
            'autoinstall_known_extensions': True,  # Enable auto-download
            'autoload_known_extensions': True,     # Enable auto-load
            'disabled_optimizers': '',       # Enable all optimizers
        }

    # High-performance settings
    DUCKDB_ARROW_EXTENSION_ENABLED: bool = os.getenv("DUCKDB_ARROW_EXTENSION_ENABLED", "True").lower() == "true"

# Global instance
settings = Settings()
