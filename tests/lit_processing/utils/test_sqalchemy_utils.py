from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from sqlalchemy import text

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_engine, \
    create_postgres_session, sqlalchemy_load_ref_xref
from ...fixtures import db # noqa
from ...api.test_cross_ref import test_cross_reference # noqa
from ...api.fixtures import auth_headers # noqa
from ...api.test_reference import test_reference # noqa


class TestSqlalchemyUtils:
    def test_create_postgres_engine(self, db, capfd): # noqa
        engine = create_postgres_engine(verbose=False)
        assert isinstance(engine, Engine)

        with engine.connect() as connection:
            res = connection.execute(text("SELECT * FROM USERS"))
            assert len([row for row in res]) > 0

        create_postgres_engine(verbose=True)
        print_out = capfd.readouterr()[0]
        assert print_out != ""

    def test_create_postgres_session(self, db, capfd): # noqa
        session = create_postgres_session(verbose=False)
        assert isinstance(session, Session)
        res = session.execute(text("SELECT * from USERS"))
        assert len([row for row in res]) > 0

        create_postgres_session(verbose=True)
        print_out = capfd.readouterr()[0]
        assert print_out != ""

    def test_sqlalchemy_load_ref_xref(self, db, test_cross_reference): # noqa
        sqlalchemy_load_ref_xref(datatype="reference")
        sqlalchemy_load_ref_xref(datatype="resource")
        res = db.execute(text("SELECT * from cross_reference"))
        assert len([row for row in res]) > 0
