"""Root conftest.

Runs before any ``agr_literature_service`` module (and therefore before
``agr_literature_service.api.config`` reads the database settings). When the
test session is parallelized with pytest-xdist, each worker is given its own
database so the workers don't collide on the shared, cleanup-disabled schema.

Without xdist (``PYTEST_XDIST_WORKER`` unset) this is a no-op, so serial runs
and the other CI jobs are unaffected.
"""
import os


def _configure_xdist_worker_db() -> None:
    worker = os.environ.get("PYTEST_XDIST_WORKER")
    if not worker:
        return

    base_db = os.environ.get("PSQL_DATABASE")
    if not base_db or base_db.endswith(f"_{worker}"):
        return
    worker_db = f"{base_db}_{worker}"

    import psycopg2

    conn = psycopg2.connect(
        dbname=base_db,
        user=os.environ["PSQL_USERNAME"],
        password=os.environ["PSQL_PASSWORD"],
        host=os.environ["PSQL_HOST"],
        port=os.environ["PSQL_PORT"],
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(f'DROP DATABASE IF EXISTS "{worker_db}"')
            cur.execute(f'CREATE DATABASE "{worker_db}"')
    finally:
        conn.close()

    # Point every engine (app + test fixtures) at this worker's database.
    os.environ["PSQL_DATABASE"] = worker_db


_configure_xdist_worker_db()
