from sqlalchemy import Column
from sqlalchemy import String

from literature.database.base import Base
#import literature.database.main
#print(literature.database.main.__file__)
#print(dir(literature.database.main))

class User(Base):
    __tablename__ = 'users'
#    __versioned__ = {}

    id = Column(
        String,
        primary_key=True,
        index=True
    )

    email = Column(
        String,
        nullable=True
    )
