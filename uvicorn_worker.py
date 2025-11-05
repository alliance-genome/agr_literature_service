"""Custom Uvicorn worker with access logging enabled."""
import logging
from uvicorn.workers import UvicornWorker as BaseUvicornWorker


class UvicornWorker(BaseUvicornWorker):
    """Custom Uvicorn worker that enables access logging."""

    CONFIG_KWARGS = {
        "loop": "auto",
        "http": "auto",
        "lifespan": "off",  # Disable lifespan (we handle DB init in gunicorn hook)
        "access_log": True,  # Enable access logging
        "log_level": "info",  # Set log level to info
    }

    def init_process(self):
        """Initialize the worker process and configure logging."""
        super().init_process()

        # Configure uvicorn's access logger to use gunicorn's log format
        access_logger = logging.getLogger("uvicorn.access")
        access_logger.setLevel(logging.INFO)

        # Ensure handlers are propagated to root logger
        if not access_logger.handlers:
            access_logger.addHandler(logging.StreamHandler())

        # Log worker initialization
        self.log.info(f"Uvicorn worker initialized (PID: {self.pid})")
