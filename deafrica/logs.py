import logging


def setup_logging(level: int = logging.INFO) -> logging.Logger:
    """Set up a simple logger"""
    log = logging.getLogger(__name__)
    console = logging.StreamHandler()
    log.addHandler(console)
    log.setLevel(level)
    return log
