import json
import logging
import logging.config
import os
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

from prometheus_client import Counter

# Metrics for logging
LOG_ENTRIES = Counter('log_entries_total', 'Total number of log entries', ['level', 'component'])

class JsonFormatter(logging.Formatter):
    """JSON formatter for structured logging"""
    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            'timestamp': datetime.utcnow().isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'component': getattr(record, 'component', 'unknown'),
            'trace_id': getattr(record, 'trace_id', None),
            'service': os.getenv('SERVICE_NAME', 'unknown')
        }

        if record.exc_info:
            log_data['exception'] = traceback.format_exception(*record.exc_info)

        # Update metrics
        LOG_ENTRIES.labels(level=record.levelname.lower(), 
                         component=log_data['component']).inc()

        return json.dumps(log_data)

def setup_logging(config: Dict[str, Any] = None) -> None:
    """Setup logging configuration"""
    default_config = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'json': {
                '()': JsonFormatter
            }
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'formatter': 'json',
                'stream': 'ext://sys.stdout'
            },
            'file': {
                'class': 'logging.handlers.RotatingFileHandler',
                'formatter': 'json',
                'filename': os.path.join(
                    'logs',
                    f'nl-to-sparql_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
                ),
                'maxBytes': 10485760,  # 10MB
                'backupCount': 5
            }
        },
        'loggers': {
            '': {  # Root logger
                'handlers': ['console', 'file'],
                'level': os.getenv('LOG_LEVEL', 'INFO'),
                'propagate': True
            }
        }
    }

    # Create logs directory if it doesn't exist
    Path('logs').mkdir(exist_ok=True)

    # Update with custom config if provided
    if config:
        default_config.update(config)

    # Apply configuration
    logging.config.dictConfig(default_config)

def get_logger(name: str, component: str = None) -> logging.Logger:
    """Get a logger with optional component name"""
    logger = logging.getLogger(name)
    
    # Add component as an extra field if provided
    if component:
        logger = logging.LoggerAdapter(logger, {'component': component})
    
    return logger

# Example usage in other modules:
# logger = get_logger(__name__, 'api')
# logger.info('Starting API server', extra={'trace_id': '123'})