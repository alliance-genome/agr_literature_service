from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy_continuum import make_versioned
#from fastapi_sqlalchemy import DBSessionMiddleware

#import databases

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from literature.config import config

Base = declarative_base()

SQLALCHEMY_DATABASE_URL = "postgresql://" \
        + config.PSQL_USERNAME + ":" + config.PSQL_PASSWORD \
        + "@" + config.PSQL_HOST + ":" + config.PSQL_PORT \
        + "/" + config.PSQL_DATABASE

metadata = MetaData()

#app.add_middleware(DBSessionMiddleware, db_url=SQLALCHEMY_DATABASE_URL)
engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"options": "-c timezone=utc"})

#database = databases.Database(SQLALCHEMY_DATABASE_URL)

SessionLocal = sessionmaker(bind=engine, autoflush=True)#, autocommit=False, autoflush=False,)



Base.metadata.create_all(engine)
make_versioned(#options={'native_versioning': True},
               user_cls=None)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
