"""
user_model.py
=============
"""


from typing import Dict

from sqlalchemy import Column, String

from agr_literature_service.api.database.base import Base


class UserModel(Base):
    __tablename__ = "users"
    __versioned__: Dict = {}

    id = Column(
        String,
        primary_key=True,
        index=True
    )

    email = Column(
        String,
        nullable=True
    )
