import logging
import os
from logging.handlers import RotatingFileHandler


def setup_logger(name: str, folder_path: str = "logs", logger_name: str = "app",
                 level: int = logging.INFO, max_bytes: int = 5 * 1024 * 1024,
                 backup_count: int = 3) -> logging.Logger:
    """
    Set up a logger with both console and file handlers, including color, emojis, and detailed caller information.

    Parameters:
    - name: Name of the logger
    - folder_path: Directory path where the log file will be created
    - logger_name: Name of the log file without extension
    - level: Logging level (default: INFO)
    - max_bytes: Max size of the log file in bytes before rotating (default: 5 MB)
    - backup_count: Number of rotated log files to keep (default: 3)

    Returns:
    - Logger instance with specified configuration
    """
    # Ensure the folder exists
    os.makedirs(folder_path, exist_ok=True)

    # Full path for the log file
    log_file = os.path.join(folder_path, f"{logger_name}.log")

    # Initialize logger
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Prevent duplicate log entries
    if logger.hasHandlers():
        logger.handlers.clear()

    # Define ANSI color codes and emojis
    COLORS = {
        "DEBUG": "\033[96m",  # Cyan
        "INFO": "\033[92m",  # Green
        "WARNING": "\033[93m",  # Yellow
        "ERROR": "\033[91m",  # Red
        "CRITICAL": "\033[95m",  # Magenta
        "RESET": "\033[0m"  # Reset to default color
    }

    EMOJIS = {
        "DEBUG": "🛠️",
        "INFO": "ℹ️",
        "WARNING": "⚠️",
        "ERROR": "❌",
        "CRITICAL": "🔥"
    }

    # Define custom log formats
    def format_log(record: logging.LogRecord) -> str:
        # Fetch color and emoji for the log level
        color = COLORS.get(record.levelname, COLORS["RESET"])
        emoji = EMOJIS.get(record.levelname, "")

        # Custom format with color and emoji
        log_format = f"{color}{emoji} %(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s{COLORS['RESET']}"
        formatter = logging.Formatter(log_format)
        return formatter.format(record)

    # Console handler with color and emoji
    class CustomConsoleHandler(logging.StreamHandler):
        def emit(self, record):
            message = format_log(record)
            self.stream.write(f"{message}\n")
            self.flush()

    console_handler = CustomConsoleHandler()
    logger.addHandler(console_handler)

    # File handler (no color but includes full info for file logging)
    file_handler = RotatingFileHandler(log_file, maxBytes=max_bytes,
                                       backupCount=backup_count)
    file_format = "%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s"
    file_handler.setFormatter(logging.Formatter(file_format))
    logger.addHandler(file_handler)

    return logger
