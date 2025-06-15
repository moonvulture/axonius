import logging
import os
from datetime import datetime
from typing import Optional, cast


class AxoniusLogger:
    """Custom logger for Axonius to Elasticsearch pipeline"""
    
    def __init__(self, name: str = __name__, log_level: str = "INFO"):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, log_level.upper()))
        
        # Prevent duplicate handlers
        if not self.logger.handlers:
            self._setup_handlers()
    
    def _setup_handlers(self):
        """Setup console and file handlers"""
        
        # Create formatters
        detailed_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        simple_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(simple_formatter)
        self.logger.addHandler(console_handler)
        
        # File handler (optional - based on environment variable)
        log_file = os.getenv('LOG_FILE')
        if log_file:
            # Create logs directory if it doesn't exist
            log_dir = os.path.dirname(log_file)
            if log_dir and not os.path.exists(log_dir):
                os.makedirs(log_dir)
            
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(detailed_formatter)
            self.logger.addHandler(file_handler)
    
    def get_logger(self):
        """Return the configured logger instance"""
        return self.logger


# Default logger instance
def get_logger(name: str = __name__, log_level: str = "INFO") -> logging.Logger:
    """
    Get a configured logger instance
    
    Args:
        name: Logger name (usually __name__)
        log_level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    
    Returns:
        Configured logger instance
    """
    log_level = log_level or os.getenv('LOG_LEVEL', 'INFO')
    axonius_logger = AxoniusLogger(name, log_level)
    return axonius_logger.get_logger()


# Context managers for operation logging
class LoggedOperation:
    """Context manager for logging operations with timing"""
    
    def __init__(self, logger: logging.Logger, operation_name: str, log_level: str = "INFO"):
        self.logger = logger
        self.operation_name = operation_name
        self.log_level = getattr(logging, log_level.upper())
        self.start_time = None
    
    def __enter__(self):
        self.start_time = datetime.now()
        self.logger.log(self.log_level, f"Starting {self.operation_name}...")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.start_time is None:
            self.logger.error(f"{self.operation_name} exited before it started")
            return False

        duration = datetime.now() - self.start_time
        
        if exc_type is None:
            self.logger.log(self.log_level, f"Completed {self.operation_name} in {duration.total_seconds():.2f} seconds")
        else:
            self.logger.error(f"Failed {self.operation_name} after {duration.total_seconds():.2f} seconds: {exc_val}")
        
        return False  # Don't suppress exceptions


# Utility functions for common logging patterns
def log_api_request(logger: logging.Logger, method: str, url: str, status_code: Optional[int] = None):
    """Log API request details"""
    if status_code:
        level = logging.INFO if 200 <= status_code < 300 else logging.WARNING
        logger.log(level, f"API {method} {url} - Status: {status_code}")
    else:
        logger.info(f"API {method} {url}")


def log_data_stats(logger: logging.Logger, operation: str, count: int, details: str = ""):
    """Log data processing statistics"""
    message = f"{operation}: {count} items"
    if details:
        message += f" ({details})"
    logger.info(message)


def log_elasticsearch_operation(logger: logging.Logger, operation: str, index: str, count: int = None):
    """Log Elasticsearch operations"""
    message = f"Elasticsearch {operation} - Index: {index}"
    if count is not None:
        message += f" - Count: {count}"
    logger.info(message)


# Export commonly used items
__all__ = [
    'get_logger',
    'AxoniusLogger', 
    'LoggedOperation',
    'log_api_request',
    'log_data_stats',
    'log_elasticsearch_operation'
]