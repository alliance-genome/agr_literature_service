from sqlalchemy.orm import configure_mappers

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import create_session, configure_mappers

from literature.database.main import Base

from literature.models.reference import Reference
from literature.models.resource import Resource
from literature.models.author import Author
from literature.models.editor import Editor

from literature.models.user import User

configure_mappers()
