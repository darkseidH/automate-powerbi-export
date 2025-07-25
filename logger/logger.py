# logger.py
import logging
import sys


class Logger:
    """Static logger configuration with class-level methods."""

    _logger = None

    @classmethod
    def setup(cls, name: str = __name__, level: int = logging.INFO) -> None:
        """Initialize logger configuration once."""
        if cls._logger is None:
            cls._logger = logging.getLogger(name)
            cls._logger.setLevel(level)

            # Console handler with formatting
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
            handler.setFormatter(formatter)
            cls._logger.addHandler(handler)

    @classmethod
    def get_logger(cls) -> logging.Logger:
        """Return singleton logger instance."""
        if cls._logger is None:
            cls.setup()
        return cls._logger

    # Direct logging methods
    @classmethod
    def info(cls, msg: str, *args, **kwargs) -> None:
        cls.get_logger().info(msg, *args, **kwargs)

    @classmethod
    def error(cls, msg: str, *args, **kwargs) -> None:
        cls.get_logger().error(msg, *args, **kwargs)

    @classmethod
    def debug(cls, msg: str, *args, **kwargs) -> None:
        cls.get_logger().debug(msg, *args, **kwargs)
