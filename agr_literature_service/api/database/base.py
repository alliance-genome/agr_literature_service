from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import MetaData
metadata = MetaData(schema='lit')
Base = declarative_base(metadata=metadata)
# Base = declarative_base()
