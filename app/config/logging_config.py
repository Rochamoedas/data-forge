import logging
import sys
from logging.handlers import RotatingFileHandler
import os
from app.config.settings import settings

def setup_logging():
    """Configure logging for the application."""
    # Create logs directory if it doesn't exist
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Create logger
    logger = logging.getLogger("app")
    logger.setLevel(logging.INFO)

    # Create formatters
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Console handler with UTF-8 encoding
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    # Set encoding to UTF-8 to handle Unicode characters
    if hasattr(console_handler.stream, 'reconfigure'):
        console_handler.stream.reconfigure(encoding='utf-8')
    logger.addHandler(console_handler)

    # File handler with rotation and UTF-8 encoding
    file_handler = RotatingFileHandler(
        os.path.join(log_dir, "app.log"),
        maxBytes=10485760,  # 10MB
        backupCount=5,
        encoding='utf-8'  # Explicitly set UTF-8 encoding for file
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger

def sanitize_log_message(message: str) -> str:
    """
    Sanitize log messages to replace Unicode characters that might cause encoding issues.
    This is a fallback for Windows systems with cp1252 encoding.
    """
    # Replace common Unicode characters with ASCII equivalents
    replacements = {
        '🚀': '[ROCKET]',
        '🔄': '[REFRESH]',
        '→': '->',
        '✅': '[CHECK]',
        '❌': '[X]',
        '⚠️': '[WARNING]',
        '📦': '[PACKAGE]',
        '🎯': '[TARGET]',
        '💾': '[DISK]',
        '⏱️': '[TIMER]',
        '📊': '[CHART]',
        '🔍': '[SEARCH]',
        '🌟': '[STAR]',
        '⚡': '[LIGHTNING]',
        '🔧': '[WRENCH]',
        '📈': '[TRENDING_UP]',
        '📉': '[TRENDING_DOWN]',
        '🎉': '[PARTY]',
        '🔥': '[FIRE]',
        '💡': '[BULB]',
        '🎪': '[CIRCUS]',
        '🏆': '[TROPHY]',
        '🎨': '[PALETTE]',
        '🚨': '[SIREN]',
        '🎭': '[MASKS]',
        '🎪': '[TENT]',
        '🎯': '[DART]',
        '🎲': '[DICE]',
        '🎮': '[GAME]',
        '🎸': '[GUITAR]',
        '🎺': '[TRUMPET]',
        '🎻': '[VIOLIN]',
        '🎹': '[PIANO]',
        '🥁': '[DRUM]',
        '🎤': '[MIC]',
        '🎧': '[HEADPHONES]',
        '🎬': '[CLAPPER]',
        '🎭': '[THEATER]',
        '🎪': '[CIRCUS_TENT]',
        '🎨': '[ART]',
        '🎯': '[BULLSEYE]',
        '🎲': '[GAME_DIE]',
        '🎮': '[VIDEO_GAME]',
        '🎸': '[ELECTRIC_GUITAR]',
        '🎺': '[TRUMPET_HORN]',
        '🎻': '[VIOLIN_BOW]',
        '🎹': '[MUSICAL_KEYBOARD]',
        '🥁': '[DRUM_SET]',
        '🎤': '[MICROPHONE]',
        '🎧': '[HEADPHONE]',
        '🎬': '[MOVIE_CAMERA]'
    }
    
    for unicode_char, ascii_replacement in replacements.items():
        message = message.replace(unicode_char, ascii_replacement)
    
    return message

class UnicodeCompatibleLogger:
    """
    A wrapper around the logger that sanitizes Unicode characters
    to prevent encoding errors on Windows systems.
    """
    def __init__(self, logger):
        self._logger = logger
    
    def info(self, message, *args, **kwargs):
        sanitized_message = sanitize_log_message(str(message))
        self._logger.info(sanitized_message, *args, **kwargs)
    
    def warning(self, message, *args, **kwargs):
        sanitized_message = sanitize_log_message(str(message))
        self._logger.warning(sanitized_message, *args, **kwargs)
    
    def error(self, message, *args, **kwargs):
        sanitized_message = sanitize_log_message(str(message))
        self._logger.error(sanitized_message, *args, **kwargs)
    
    def debug(self, message, *args, **kwargs):
        sanitized_message = sanitize_log_message(str(message))
        self._logger.debug(sanitized_message, *args, **kwargs)
    
    def critical(self, message, *args, **kwargs):
        sanitized_message = sanitize_log_message(str(message))
        self._logger.critical(sanitized_message, *args, **kwargs)

# Create and configure the logger
_base_logger = setup_logging()
logger = UnicodeCompatibleLogger(_base_logger) 