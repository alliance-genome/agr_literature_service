
from sqlalchemy import create_engine
from sqlalchemy import MetaData

from fastapi import Depends

from literature.database.base import Base
from literature.database.config import SQLALCHEMY_DATABASE_URL



from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import Session

metadata = MetaData()

engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"options": "-c timezone=utc"})
print(SQLALCHEMY_DATABASE_URL)
Base.metadata.create_all(engine)

SessionLocal = sessionmaker(bind=engine, autoflush=True)


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
