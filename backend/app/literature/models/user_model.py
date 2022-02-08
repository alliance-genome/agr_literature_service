from sqlalchemy import Column, String

from literature.database.base import Base


class UserModel(Base):
    __tablename__ = 'users'
    __versioned__ = {}

    id = Column(
        String,
        primary_key=True,
        index=True
    )

    email = Column(
        String,
        nullable=True
    )
