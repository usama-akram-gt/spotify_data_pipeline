#!/usr/bin/env python3
"""
Logging utilities for the Spotify data pipeline.

This module provides standardized logging configuration and utilities:
- Centralized logging configuration
- Custom formatters and handlers
- JSON logging for better integration with log aggregation tools
"""

import json
import logging
import os
import sys
import time
from datetime import datetime
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
from pathlib import Path
from typing import Dict, Any, Optional, Union

# Default log format
DEFAULT_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_LOG_DIR = "logs"

# Ensure log directory exists
os.makedirs(DEFAULT_LOG_DIR, exist_ok=True)


class JsonFormatter(logging.Formatter):
    """Formatter for JSON-structured log records."""

    def __init__(self, include_extra: bool = True):
        """
        Initialize the JSON formatter.
        
        Args:
            include_extra: Whether to include extra fields in the log record
        """
        self.include_extra = include_extra
        super().__init__()

    def format(self, record: logging.LogRecord) -> str:
        """
        Format the log record as JSON.
        
        Args:
            record: Log record to format
            
        Returns:
            JSON-formatted log string
        """
        log_data = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "line": record.lineno,
            "process": record.process
        }

        # Include exception info if available
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        # Include extra fields if available and enabled
        if self.include_extra and hasattr(record, "extra"):
            log_data.update(record.extra)

        # Add any additional attributes set by LoggerAdapter
        for attr in dir(record):
            if attr.startswith("_") or attr in [
                "args", "asctime", "created", "exc_info", "exc_text", 
                "filename", "funcName", "id", "levelname", "levelno", 
                "lineno", "module", "msecs", "message", "msg", "name", 
                "pathname", "process", "processName", "relativeCreated", 
                "stack_info", "thread", "threadName"
            ]:
                continue
            log_data[attr] = getattr(record, attr)

        return json.dumps(log_data)


class PipelineLoggerAdapter(logging.LoggerAdapter):
    """Logger adapter that adds pipeline-specific context to log records."""

    def __init__(self, logger: logging.Logger, pipeline_name: str, 
                extra: Optional[Dict[str, Any]] = None):
        """
        Initialize the logger adapter.
        
        Args:
            logger: Base logger to adapt
            pipeline_name: Name of the pipeline
            extra: Additional context fields
        """
        extra = extra or {}
        extra["pipeline"] = pipeline_name
        super().__init__(logger, extra)

    def process(self, msg: str, kwargs: Dict[str, Any]) -> tuple:
        """
        Process the log message.
        
        Args:
            msg: Log message
            kwargs: Additional keyword arguments
            
        Returns:
            Processed message and kwargs
        """
        # Add pipeline info to the message
        if "extra" not in kwargs:
            kwargs["extra"] = {}
        kwargs["extra"].update(self.extra)
        return msg, kwargs


def configure_logger(
    name: str,
    level: str = DEFAULT_LOG_LEVEL,
    log_file: Optional[str] = None,
    log_format: str = DEFAULT_LOG_FORMAT,
    json_format: bool = False,
    rotate: bool = True,
    max_bytes: int = 10485760,  # 10 MB
    backup_count: int = 5
) -> logging.Logger:
    """
    Configure and return a logger.
    
    Args:
        name: Logger name
        level: Log level
        log_file: Path to log file (None for console only)
        log_format: Log format string
        json_format: Whether to format logs as JSON
        rotate: Whether to use rotating file handler
        max_bytes: Maximum file size for rotation
        backup_count: Number of backup files to keep
        
    Returns:
        Configured logger
    """
    # Get the logger
    logger = logging.getLogger(name)
    
    # Set the log level
    level_map = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL
    }
    log_level = level_map.get(level.upper(), logging.INFO)
    logger.setLevel(log_level)
    
    # Remove existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Create formatter
    if json_format:
        formatter = JsonFormatter()
    else:
        formatter = logging.Formatter(log_format)
    
    # Add console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Add file handler if specified
    if log_file:
        # Ensure log directory exists
        os.makedirs(os.path.dirname(os.path.abspath(log_file)), exist_ok=True)
        
        if rotate:
            file_handler = RotatingFileHandler(
                log_file,
                maxBytes=max_bytes,
                backupCount=backup_count
            )
        else:
            file_handler = logging.FileHandler(log_file)
        
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


def get_pipeline_logger(
    pipeline_name: str,
    level: str = DEFAULT_LOG_LEVEL,
    log_to_file: bool = True,
    json_format: bool = False
) -> logging.LoggerAdapter:
    """
    Get a logger for a specific pipeline.
    
    Args:
        pipeline_name: Name of the pipeline
        level: Log level
        log_to_file: Whether to log to a file
        json_format: Whether to format logs as JSON
        
    Returns:
        Logger adapter for the pipeline
    """
    log_file = None
    if log_to_file:
        log_file = os.path.join(DEFAULT_LOG_DIR, f"{pipeline_name}.log")
    
    logger = configure_logger(
        f"spotify.pipeline.{pipeline_name}",
        level=level,
        log_file=log_file,
        json_format=json_format
    )
    
    return PipelineLoggerAdapter(logger, pipeline_name)


def log_execution_time(logger: Union[logging.Logger, logging.LoggerAdapter], 
                      operation_name: str):
    """
    Decorator to log execution time of a function.
    
    Args:
        logger: Logger to use
        operation_name: Name of the operation for logging
        
    Returns:
        Decorated function
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            start_time = time.time()
            logger.info(f"Starting {operation_name}")
            
            try:
                result = func(*args, **kwargs)
                execution_time = time.time() - start_time
                logger.info(f"Completed {operation_name} in {execution_time:.2f} seconds")
                return result
            except Exception as e:
                execution_time = time.time() - start_time
                logger.error(f"Failed {operation_name} after {execution_time:.2f} seconds: {str(e)}", exc_info=True)
                raise
                
        return wrapper
    return decorator


class StatusLogger:
    """Helper class for logging status updates with progress tracking."""
    
    def __init__(self, logger: Union[logging.Logger, logging.LoggerAdapter], 
                total: int, operation_name: str, log_interval: int = 100):
        """
        Initialize the status logger.
        
        Args:
            logger: Logger to use
            total: Total number of items to process
            operation_name: Name of the operation for logging
            log_interval: How often to log progress (in number of items)
        """
        self.logger = logger
        self.total = total
        self.operation_name = operation_name
        self.log_interval = log_interval
        self.processed = 0
        self.start_time = None
        self.last_log_time = None
    
    def __enter__(self):
        """Start the operation and logging."""
        self.start_time = time.time()
        self.last_log_time = self.start_time
        self.logger.info(f"Starting {self.operation_name} for {self.total} items")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Complete the operation and log final status."""
        end_time = time.time()
        total_time = end_time - self.start_time
        
        if exc_type:
            self.logger.error(
                f"Failed {self.operation_name} after processing {self.processed}/{self.total} items "
                f"in {total_time:.2f} seconds: {str(exc_val)}",
                exc_info=True
            )
        else:
            self.logger.info(
                f"Completed {self.operation_name} for {self.processed}/{self.total} items "
                f"in {total_time:.2f} seconds ({self.processed/total_time:.2f} items/sec)"
            )
    
    def update(self, count: int = 1, extra_info: Optional[Dict[str, Any]] = None):
        """
        Update progress and log if needed.
        
        Args:
            count: Number of items processed in this update
            extra_info: Additional information to log
        """
        self.processed += count
        current_time = time.time()
        
        # Log based on interval or if a significant amount of time has passed
        if (self.processed % self.log_interval == 0 or 
            current_time - self.last_log_time > 10):  # Log at least every 10 seconds
            
            elapsed = current_time - self.start_time
            progress = (self.processed / self.total) * 100 if self.total > 0 else 0
            rate = self.processed / elapsed if elapsed > 0 else 0
            
            remaining = (self.total - self.processed) / rate if rate > 0 else 0
            
            log_msg = (
                f"Progress: {self.processed}/{self.total} ({progress:.1f}%) "
                f"- Rate: {rate:.2f} items/sec - Est. remaining: {remaining:.2f} sec"
            )
            
            if extra_info:
                log_msg += f" - Info: {extra_info}"
                
            self.logger.info(log_msg)
            self.last_log_time = current_time 