import logging
import sys
from typing import Optional

# Create logger
logger = logging.getLogger("econuy")

def configure_logging(
    level: int = logging.INFO,
    handler: Optional[logging.Handler] = None,
    formatter: Optional[logging.Formatter] = None,
    disable: bool = False
) -> None:
    """Configure logging for the econuy package.

    Parameters
    ----------
    level : int
        The logging level to use. Defaults to logging.INFO.
        Set to logging.WARNING or higher to suppress most logs.
    handler : Optional[logging.Handler]
        Custom handler to use. If None, StreamHandler(sys.stdout) will be used.
    formatter : Optional[logging.Formatter]
        Custom formatter to use. If None, a default formatter will be used.
    disable : bool
        If True, all logs will be disabled. Default is False.
    """
    # Remove any existing handlers
    logger.handlers.clear()

    if disable:
        # Add a null handler to suppress all logs
        logger.addHandler(logging.NullHandler())
        logger.setLevel(logging.CRITICAL + 1)
        return

    # Use provided handler or create default
    log_handler = handler or logging.StreamHandler(sys.stdout)

    # Use provided formatter or create default
    log_formatter = formatter or logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    log_handler.setFormatter(log_formatter)
    logger.addHandler(log_handler)
    logger.setLevel(level)

# Configure default logging (enabled by default)
configure_logging()
