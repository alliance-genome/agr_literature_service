"""
user_model.py
=============
"""


from typing import Dict

from sqlalchemy import Column, String, UniqueConstraint

from agr_literature_service.api.database.base import Base


class UserModel(Base):
    __tablename__ = "users"
    __bind_key__ = 'lit'
    __versioned__: Dict = {'schema': 'lit', 'inherit': True}

    id = Column(
        String,
        primary_key=True,
        index=True
    )

    email = Column(
        String,
        nullable=True
    )

    __table_args__ = (
        UniqueConstraint(
            'id',
            name='users_unique'),
        {"schema": "lit"}
    )
