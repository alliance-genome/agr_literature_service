from sqlalchemy import create_engine
from sqlalchemy import MetaData

from fastapi import Depends

from literature.database.base import Base
from literature.database.config import SQLALCHEMY_DATABASE_URL

from literature.continuum_plugins import UserPlugin

from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import Session

from sqlalchemy_continuum import make_versioned
from sqlalchemy_continuum.plugins import PropertyModTrackerPlugin


metadata = MetaData()

engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"options": "-c timezone=utc"})
print(SQLALCHEMY_DATABASE_URL)
Base.metadata.create_all(engine)

SessionLocal = sessionmaker(bind=engine, autoflush=True)

user_plugin = UserPlugin()

make_versioned(user_cls='UserModel',
               plugins=[user_plugin, PropertyModTrackerPlugin()])



def get_db():
    db = SessionLocal()
    try:
        yield db
    except Exception as e:
        print('Error: ' + str(type(e)))
    finally:
        db.close()


def is_database_online(session: Session = Depends(get_db)):
    return {"database": "online"} if session else False
