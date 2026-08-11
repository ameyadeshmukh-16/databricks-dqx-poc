"""
Structured logging — replaces print() calls throughout the codebase.

Databricks captures Python logging output in the driver log with timestamps
and log levels, making it searchable and filterable unlike raw print() output.

Usage:
    from src.logger import get_logger
    logger = get_logger(__name__)
    logger.info("Stream started")
    logger.error("Delivery failed: %s", err)
"""

import logging
import sys


def get_logger(name: str) -> logging.Logger:
    """Return a named logger with a stdout StreamHandler (idempotent)."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(
            "%(asctime)s [%(name)s] %(levelname)s — %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        ))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger
