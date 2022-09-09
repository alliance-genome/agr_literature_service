
from sqlalchemy import create_engine
from sqlalchemy import MetaData

from fastapi import Depends

from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.config import SQLALCHEMY_DATABASE_URL

from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import Session


metadata = MetaData()

engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"options": "-c timezone=utc"})
SessionLocal = sessionmaker(bind=engine, autoflush=True)


def create_all_tables():
    Base.metadata.create_all(engine)


def create_default_user():
    engine.connect().execute("INSERT INTO users (id) VALUES ('default_user') ON CONFLICT DO NOTHING")


def get_db():
    db = SessionLocal()
    try:
        yield db
    except Exception as e:
        print('Error: ' + str(type(e)))
    finally:
        db.close()


db_session = Depends(get_db)


def is_database_online(session: Session = db_session):
    return {"database": "online"} if session else False
