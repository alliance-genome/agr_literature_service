from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import text

from fastapi import Depends

from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.config import SQLALCHEMY_DATABASE_URL

from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import Session
from agr_literature_service.api.triggers.triggers import add_sql_triggers_functions

metadata = MetaData()

engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"options": "-c timezone=utc"})
# In SQLAlchemy 2.x, sessionmaker(bind=engine) is deprecated.
# Remove bind argument, bind engine directly
SessionLocal = sessionmaker(engine, autoflush=True)


def create_all_tables():
    Base.metadata.create_all(engine)


def create_default_user():
    db = sessionmaker(engine, autoflush=True)
    with db.begin() as session:
        session.execute(text("INSERT INTO users (id, automation_username) VALUES ('default_user', 'default_user') ON CONFLICT DO NOTHING"))


def get_db():
    db = SessionLocal()
    try:
        yield db
    except Exception as e:
        db.rollback()
        print(f"Error in get_db: {e}")
        raise
    finally:
        db.close()


db_session = Depends(get_db)


def is_database_online(session: Session = db_session):
    return {"database": "online"} if session else False


def create_all_triggers():
    # Explicit session handling in SQLAlchemy 2.x
    with SessionLocal() as session:
        add_sql_triggers_functions(session)
        session.commit()  # commit after adding the triggers


def drop_open_db_sessions(db):
    com = '''SELECT pg_terminate_backend(pg_stat_activity.pid)
             FROM pg_stat_activity
             WHERE datname = current_database()
             AND pid <> pg_backend_pid()
             AND backend_type NOT IN ('walsender', 'logical replication worker')
             AND application_name != 'PostgreSQL JDBC Driver';'''
    db.execute(text(com))
    db.commit()  # commit after executing the SQL
    print(f"Closing {db}")
