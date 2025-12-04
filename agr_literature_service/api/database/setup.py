import logging
import os
from sqlalchemy import text
from agr_literature_service.api.initialize import setup_resource_descriptor
from agr_literature_service.api.models import initialize
from agr_literature_service.api.database.main import engine

logger = logging.getLogger(__name__)

# PostgreSQL advisory lock ID for database initialization
# Using a large random number to avoid conflicts
INIT_LOCK_ID = 8675309


def setup_database():
    """
    Initialize database with advisory lock to prevent concurrent initialization.
    Uses PostgreSQL advisory locks to ensure only one process initializes at a time.
    """
    pid = os.getpid()
    logger.info(f"[PID:{pid}] Attempting to acquire database initialization lock...")

    # Get a connection from the engine
    with engine.connect() as connection:
        # Try to acquire PostgreSQL advisory lock
        # pg_try_advisory_lock returns True if lock acquired, False otherwise
        result = connection.execute(
            text("SELECT pg_try_advisory_lock(:lock_id)"),
            {"lock_id": INIT_LOCK_ID}
        )
        lock_acquired = result.scalar()

        if lock_acquired:
            logger.info(f"[PID:{pid}] Lock acquired. Performing database initialization...")
            try:
                # Perform initialization
                initialize()
                setup_resource_descriptor()
                logger.info(f"[PID:{pid}] Database initialization completed successfully")
            except Exception as e:
                logger.error(f"[PID:{pid}] Database initialization failed: {e}", exc_info=True)
                raise
            finally:
                # Release the advisory lock
                connection.execute(
                    text("SELECT pg_advisory_unlock(:lock_id)"),
                    {"lock_id": INIT_LOCK_ID}
                )
                logger.info(f"[PID:{pid}] Lock released")
        else:
            # Another process is already initializing, wait for it
            logger.info(f"[PID:{pid}] Another process is initializing database. Waiting for lock...")

            # Block until we can acquire the lock (which means initialization is done)
            connection.execute(
                text("SELECT pg_advisory_lock(:lock_id)"),
                {"lock_id": INIT_LOCK_ID}
            )
            # Immediately release it since we just wanted to wait
            connection.execute(
                text("SELECT pg_advisory_unlock(:lock_id)"),
                {"lock_id": INIT_LOCK_ID}
            )
            logger.info(f"[PID:{pid}] Database initialization completed by another process")

        # Commit to ensure connection cleanup
        connection.commit()
