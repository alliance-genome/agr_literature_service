"""Gunicorn configuration for multi-process FastAPI deployment."""
import multiprocessing
import os

# Server socket
bind = "0.0.0.0:8080"
backlog = 2048

# Worker processes
# Default: (2 x CPU cores) + 1
# Can be overridden with GUNICORN_WORKERS environment variable
workers = int(os.getenv('GUNICORN_WORKERS', (multiprocessing.cpu_count() * 2) + 1))
worker_class = 'uvicorn_worker.UvicornWorker'  # Custom worker with access logging enabled
worker_connections = 1000
timeout = 120
keepalive = 5

# Restart workers after this many requests to prevent memory leaks
max_requests = 1000
max_requests_jitter = 100

# Logging
accesslog = '-'  # Log to stdout
errorlog = '-'   # Log to stderr
loglevel = os.getenv('LOG_LEVEL', 'info').lower()

# Access log format - includes worker PID to identify which worker handled request
access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s" [PID:%(p)s]'

# Process naming
proc_name = 'agr_literature_api'

# Server mechanics
daemon = False
pidfile = None
umask = 0
user = None
group = None
tmp_upload_dir = None

# Preload application code before worker processes are forked
# This ensures on_starting hook completes before workers spawn
preload_app = True

# SSL (if needed)
# keyfile = None
# certfile = None


def pre_fork(server, worker):  # noqa: ARG001
    """Called just before a worker is forked."""
    pass


def post_fork(server, worker):
    """Called just after a worker has been forked."""
    server.log.info("Worker spawned (pid: %s)", worker.pid)


def pre_exec(server):
    """Called just before a new master process is forked."""
    server.log.info("Forked child, re-executing.")


def on_starting(server):
    """Called just before the master process is initialized."""
    # Configure logging for uvicorn.access
    import logging
    access_logger = logging.getLogger("uvicorn.access")
    access_logger.setLevel(logging.INFO)
    access_logger.propagate = True  # Ensure logs propagate to root logger

    server.log.info("Initializing database...")
    from agr_literature_service.api.database.setup import setup_database
    setup_database()
    server.log.info("Database initialized successfully")


def when_ready(server):
    """Called just after the server is started."""
    server.log.info("Server is ready. Spawning workers")


def worker_int(worker):
    """Called when a worker receives the SIGINT or SIGQUIT signal."""
    worker.log.info("Worker received INT or QUIT signal")


def worker_abort(worker):
    """Called when a worker receives the SIGABRT signal."""
    worker.log.info("Worker received SIGABRT signal")
