"""Custom Uvicorn worker with access logging enabled."""
from uvicorn.workers import UvicornWorker as BaseUvicornWorker


class UvicornWorker(BaseUvicornWorker):
    """Custom Uvicorn worker that enables access logging."""

    CONFIG_KWARGS = {
        "loop": "auto",
        "http": "auto",
        "lifespan": "off",  # Disable lifespan (we handle DB init in gunicorn hook)
        "access_log": True,  # Enable access logging
    }
