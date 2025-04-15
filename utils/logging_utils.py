import json
import logging
import os
import sys
from datetime import datetime
from typing import List, Optional

# Default log levels
DEFAULT_CONSOLE_LEVEL = logging.INFO
DEFAULT_FILE_LEVEL = logging.DEBUG

# ANSI color codes for colored console output
COLORS = {
    "RESET": "\033[0m",
    "RED": "\033[31m",
    "GREEN": "\033[32m",
    "YELLOW": "\033[33m",
    "BLUE": "\033[34m",
    "MAGENTA": "\033[35m",
    "CYAN": "\033[36m",
    "WHITE": "\033[37m",
    "BOLD": "\033[1m"
}

# Map log levels to colors
LEVEL_COLORS = {
    logging.DEBUG: COLORS["BLUE"],
    logging.INFO: COLORS["GREEN"],
    logging.WARNING: COLORS["YELLOW"],
    logging.ERROR: COLORS["RED"],
    logging.CRITICAL: COLORS["BOLD"] + COLORS["RED"]
}

class ColoredFormatter(logging.Formatter):
    """Custom formatter adding colors to log level names in console output"""
    
    def format(self, record):
        # Add color to the level name if it has a color mapping
        if record.levelno in LEVEL_COLORS:
            level_color = LEVEL_COLORS[record.levelno]
            record.levelname = f"{level_color}{record.levelname}{COLORS['RESET']}"
        return super().format(record)

class JSONFormatter(logging.Formatter):
    """Custom formatter outputting logs as JSON objects"""
    
    def format(self, record):
        log_data = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "name": record.name,
            "message": record.getMessage()
        }
        
        # Add exception info if available
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        
        # Add extra fields
        for key, value in record.__dict__.items():
            if key not in log_data and not key.startswith("_") and key != "args":
                log_data[key] = value
        
        return json.dumps(log_data)

def setup_logging(
    app_name: str = "nl-to-sparql",
    console_level: int = DEFAULT_CONSOLE_LEVEL,
    file_level: int = DEFAULT_FILE_LEVEL,
    log_dir: str = "logs",
    enable_json: bool = False,
    enable_colors: bool = True
) -> logging.Logger:
    """
    Set up logging configuration with console and file handlers.
    
    Args:
        app_name: Name of the application (used for logger and log file names)
        console_level: Log level for console output
        file_level: Log level for file output
        log_dir: Directory to store log files
        enable_json: Whether to output logs as JSON objects
        enable_colors: Whether to use colored output in console
        
    Returns:
        Configured logger instance
    """
    # Create logs directory if it doesn"t exist
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # Create root logger
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)  # Capture all levels, handlers will filter
    
    # Remove existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Configure console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(console_level)
    
    if enable_colors:
        console_format = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        console_formatter = ColoredFormatter(console_format)
    else:
        console_format = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        console_formatter = logging.Formatter(console_format)
        
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    # Configure file handler
    log_filename = f"{app_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    log_filepath = os.path.join(log_dir, log_filename)
    
    file_handler = logging.FileHandler(log_filepath)
    file_handler.setLevel(file_level)
    
    if enable_json:
        file_formatter = JSONFormatter()
    else:
        file_format = "%(asctime)s | %(levelname)s | %(name)s | %(filename)s:%(lineno)d | %(message)s"
        file_formatter = logging.Formatter(file_format)
        
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
    
    # Create a specific logger for the application
    app_logger = logging.getLogger(app_name)
    app_logger.info(f"Logging initialized: console={logging.getLevelName(console_level)}, file={logging.getLevelName(file_level)}")
    
    return app_logger

def get_logger(name: str) -> logging.Logger:
    """
    Get a logger with the given name.
    
    Args:
        name: Name of the logger
        
    Returns:
        Logger instance
    """
    return logging.getLogger(name)

class LogCapture:
    """Context manager to capture log output for testing or debugging"""
    
    def __init__(self, logger_name: str, level: int = logging.DEBUG):
        """
        Initialize log capture.
        
        Args:
            logger_name: Name of the logger to capture
            level: Minimum log level to capture
        """
        self.logger_name = logger_name
        self.level = level
        self.captured = []
        self.handler = None
    
    def __enter__(self):
        """Set up log capture when entering context"""
        logger = logging.getLogger(self.logger_name)
        
        class CaptureHandler(logging.Handler):
            def __init__(self, captured_records):
                super().__init__()
                self.captured_records = captured_records
                
            def emit(self, record):
                self.captured_records.append(record)
        
        self.handler = CaptureHandler(self.captured)
        self.handler.setLevel(self.level)
        logger.addHandler(self.handler)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Clean up when exiting context"""
        if self.handler:
            logger = logging.getLogger(self.logger_name)
            logger.removeHandler(self.handler)
    
    def get_messages(self) -> List[str]:
        """Get captured log messages"""
        return [record.getMessage() for record in self.captured]
    
    def get_records(self) -> List[logging.LogRecord]:
        """Get captured log records"""
        return self.captured

def log_execution_time(logger: Optional[logging.Logger] = None):
    """
    Decorator to log execution time of functions.
    
    Args:
        logger: Logger to use (gets a new one if None)
    """
    import functools
    import time
    
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            func_logger = logger or logging.getLogger(func.__module__)
            
            start_time = time.time()
            result = func(*args, **kwargs)
            end_time = time.time()
            
            execution_time = end_time - start_time
            func_logger.debug(f"Function '{func.__name__}' executed in {execution_time:.4f} seconds")
            
            return result
        return wrapper
    return decorator
