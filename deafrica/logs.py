import logging
import sys


def setup_logging(level: int = logging.INFO) -> logging.Logger:
    """Set up a simple logger"""
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    if not root_logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            "%(asctime)s %(name)s [%(levelname)s]: %(message)s"
        )
        handler.setFormatter(formatter)
        root_logger.addHandler(handler)

    return logging.getLogger(__name__)
