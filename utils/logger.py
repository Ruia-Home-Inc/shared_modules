import logging
import logging.handlers
import os
import sys
from datetime import datetime
from typing import Optional

from app.core.config import settings



class ColoredFormatter(logging.Formatter):
    """Colored formatter for console logging"""
    
    COLORS = {
        'DEBUG': '\033[94m',      # Blue
        'INFO': '\033[92m',       # Green
        'WARNING': '\033[93m',    # Yellow
        'ERROR': '\033[91m',      # Red
        'CRITICAL': '\033[95m',   # Magenta
    }
    RESET = '\033[0m'
    
    def format(self, record):
        log_color = self.COLORS.get(record.levelname, self.RESET)
        record.levelname = f"{log_color}{record.levelname}{self.RESET}"
        record.name = f"\033[96m{record.name}{self.RESET}"  # Cyan for logger name
        return super().format(record)


def setup_logging(
    app_name: str = "item-master",
    log_level: str = "INFO",
    log_file: Optional[str] = None,
    enable_console: bool = True,
    enable_file: bool = True,
    max_file_size: int = 10 * 1024 * 1024,  # 10MB
    backup_count: int = 5
) -> logging.Logger:
    """
    Setup comprehensive logging configuration
    
    Args:
        app_name: Name of the application
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Log file path (if None, uses default)
        enable_console: Enable console logging
        enable_file: Enable file logging  
        max_file_size: Maximum size of log file before rotation
        backup_count: Number of backup files to keep
    
    Returns:
        Configured logger instance
    """
    
    # Create logs directory if it doesn't exist
    log_dir = os.path.join(os.getcwd(), "logs")
    os.makedirs(log_dir, exist_ok=True)
    
    if log_file is None:
        log_file = os.path.join(log_dir, f"{app_name}.log")
    
    # Create logger
    logger = logging.getLogger(app_name)
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Clear existing handlers to avoid duplication
    logger.handlers.clear()
    
    # Console formatter with colors
    console_formatter = ColoredFormatter(
        fmt='%(asctime)s | %(name)s | %(levelname)s | %(filename)s:%(lineno)d | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # File formatter (no colors)
    file_formatter = logging.Formatter(
        fmt='%(asctime)s | %(name)s | %(levelname)s | %(filename)s:%(lineno)d | %(funcName)s() | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console handler
    if enable_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, log_level.upper()))
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
    
    # File handler with rotation
    if enable_file:
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=max_file_size,
            backupCount=backup_count,
            encoding='utf-8'
        )
        file_handler.setLevel(logging.DEBUG)  # File always logs everything
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
    
    # Error file handler (only errors and critical)
    error_log_file = os.path.join(log_dir, f"{app_name}_errors.log")
    error_handler = logging.handlers.RotatingFileHandler(
        error_log_file,
        maxBytes=max_file_size,
        backupCount=backup_count,
        encoding='utf-8'
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(file_formatter)
    logger.addHandler(error_handler)
    
    # Configure third-party loggers
    configure_third_party_loggers(log_level)
    
    logger.info(f"Logging initialized for {app_name}")
    logger.info(f"Log level: {log_level}")
    logger.info(f"Log file: {log_file}")
    logger.info(f"Error log file: {error_log_file}")
    
    return logger

def configure_third_party_loggers(log_level: str = "INFO"):
    """Configure logging for third-party libraries"""
    
    # SQLAlchemy
    sqlalchemy_logger = logging.getLogger('sqlalchemy.engine')
    sqlalchemy_logger.setLevel(logging.WARNING if log_level != "DEBUG" else logging.INFO)
    
    # HTTPX
    httpx_logger = logging.getLogger('httpx')
    httpx_logger.setLevel(logging.WARNING)
    
    # AsyncPG
    asyncpg_logger = logging.getLogger('asyncpg')
    asyncpg_logger.setLevel(logging.WARNING)
    
    # Motor (MongoDB)
    motor_logger = logging.getLogger('motor')
    motor_logger.setLevel(logging.WARNING)
    
    # Uvicorn
    uvicorn_logger = logging.getLogger('uvicorn')
    uvicorn_logger.setLevel(logging.INFO)
    
    # Fastapi
    fastapi_logger = logging.getLogger('fastapi')
    fastapi_logger.setLevel(logging.INFO)

def get_logger(name: str) -> logging.Logger:
    """Get a logger instance with the given name"""
    return logging.getLogger(name)

def log_request_info(logger: logging.Logger, method: str, url: str, status_code: int, duration: float):
    """Log HTTP request information"""
    if status_code >= 500:
        logger.error(f"{method} {url} - {status_code} - {duration:.2f}s")
    elif status_code >= 400:
        logger.warning(f"{method} {url} - {status_code} - {duration:.2f}s")
    else:
        logger.info(f"{method} {url} - {status_code} - {duration:.2f}s")

def log_database_operation(logger: logging.Logger, operation: str, table: str, duration: float, success: bool = True):
    """Log database operation"""
    if success:
        logger.debug(f"DB {operation} on {table} completed in {duration:.2f}s")
    else:
        logger.error(f"DB {operation} on {table} failed after {duration:.2f}s")

def log_service_call(logger: logging.Logger, service: str, endpoint: str, duration: float, success: bool = True):
    """Log inter-service communication"""
    if success:
        logger.info(f"Service call to {service}{endpoint} completed in {duration:.2f}s")
    else:
        logger.error(f"Service call to {service}{endpoint} failed after {duration:.2f}s")


# Global logger instance
app_logger = setup_logging(
    app_name="backend-service",
    log_level=settings.log_level,
    enable_console=not settings.production,
    enable_file=True
) 