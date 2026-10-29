"""Create and fill the A-team fixture tables in a fresh test database (SCRUM-6720).

ABC tables have foreign keys to A-team's ontologyterm on the ateam_merge branch, so the API's
create_all_tables() fails on a database without it. The unit tests create these tables in
tests/fixtures._get_engine(); the functional and Debezium tests start from tests/init_test_db.sh
instead, which runs this after creating the empty database:

    python3 -m tests.init_ateam_fixture_db

Same DDL and seed (seed_test_terms) as the unit tests.
"""
from sqlalchemy import create_engine

from agr_literature_service.api.database.config import SQLALCHEMY_DATABASE_URL

from .ateam_fixture_schema import create_ateam_fixture_tables, seed_test_terms
from .fixtures import atp_test_hierarchy


def main() -> None:
    atp_to_name, _name_to_atp, children, _parent = atp_test_hierarchy()
    engine = create_engine(SQLALCHEMY_DATABASE_URL)
    with engine.begin() as conn:
        create_ateam_fixture_tables(conn)
        seed_test_terms(conn, children, atp_to_name)
    engine.dispose()
    print("A-team fixture tables created and seeded")


if __name__ == "__main__":
    main()
