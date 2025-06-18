import logging


def setup_logging(level: int = logging.INFO) -> logging.Logger:
    """Set up a simple logger"""
    log = logging.getLogger(__name__)
    log.setLevel(level)

    if not log.handlers:
        console = logging.StreamHandler()
        log.addHandler(console)

    return log
