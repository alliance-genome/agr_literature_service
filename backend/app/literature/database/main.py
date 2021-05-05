from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy_continuum import make_versioned
from sqlalchemy.ext.declarative import declarative_base

from literature.database.config import SQLALCHEMY_DATABASE_URL

Base = declarative_base()

metadata = MetaData()

engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"options": "-c timezone=utc"})
Base.metadata.create_all(engine)

make_versioned(#options={'native_versioning': True},
               user_cls=None)
