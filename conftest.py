"""Root conftest.

Runs before any ``agr_literature_service`` module (and therefore before
``agr_literature_service.api.config`` reads the database settings). When the
test session is parallelized with pytest-xdist, each worker is given its own
database so the workers don't collide on the shared, cleanup-disabled schema.

Without xdist (``PYTEST_XDIST_WORKER`` unset) this is a no-op, so serial runs
and the other CI jobs are unaffected.
"""
import os


def _create_worker_db() -> None:
    """Provision a per-worker database and point the engines at it.

    Done at import time, before ``agr_literature_service.api.config`` is read,
    so both the app engine and the test fixtures connect to the worker's own
    database. Schema creation happens later (see ``pytest_collection_finish``).
    """
    worker = os.environ.get("PYTEST_XDIST_WORKER")
    if not worker:
        return

    # Never run the destructive per-worker DROP/CREATE DATABASE against a real
    # (stage/prod) host. Mirrors the guard in the `db` fixture, which this path
    # bypasses because it runs at import time before any fixture.
    if "rds.amazonaws.com" in os.environ.get("PSQL_HOST", ""):
        import pytest

        pytest.exit("Refusing to create per-worker test databases on an RDS host")

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

    os.environ["PSQL_DATABASE"] = worker_db

    # Give each worker its own on-disk tmp directory too. XML_PATH is a shared
    # scratch dir that several tests both create (init_tmp_dir) and delete
    # (cleanup_tmp_files_when_done -> rmtree); without per-worker isolation one
    # worker's teardown rmtree pulls the directory out from under another.
    xml_path = os.environ.get("XML_PATH")
    if xml_path and not xml_path.rstrip("/").endswith(f"_{worker}"):
        os.environ["XML_PATH"] = xml_path.rstrip("/") + f"_{worker}/"


def pytest_collection_finish(session):
    """Build the worker's schema once collection is complete.

    Schema creation is otherwise triggered lazily by the ``db`` fixture, so a
    TestClient-only test that runs first on a fresh worker database would hit a
    schema-less DB. It must run *after* collection, though: test-only tables
    (e.g. ``audited_dummy`` in test_audited_model.py) are only registered on
    ``Base.metadata`` once their modules are imported during collection.
    Priming the shared fixtures engine here also caches it so the ``db``
    fixture does not re-initialize.
    """
    if not os.environ.get("PYTEST_XDIST_WORKER"):
        return
    from tests import fixtures

    fixtures._get_engine()


_create_worker_db()
