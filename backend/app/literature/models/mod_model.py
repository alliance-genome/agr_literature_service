"""
mod_model.py
===============
"""


from datetime import datetime
from typing import Dict

import pytz
from sqlalchemy import ARRAY, Column, DateTime, Integer, String
from sqlalchemy.orm import relationship

from literature.database.base import Base


class ModModel(Base):
    __tablename__ = "mods"
    __versioned__: Dict = {}

    mod_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    mod_corpus_associations = relationship(
        "ModCorpusAssociationModel",
        lazy="joined",
        back_populates="mod",
        cascade="all, delete, delete-orphan"
    )

    abbreviation = Column(
        String(),
        unique=True,
        nullable=False
    )

    short_name = Column(
        String(),
        unique=True,
        nullable=False
    )

    full_name = Column(
        String(),
        unique=True,
        nullable=False
    )
   

    date_updated = Column(
        DateTime,
        nullable=True,
        default=datetime.utcnow
    )

    date_created = Column(
        DateTime,
        nullable=False,
        default=datetime.now(tz=pytz.timezone("UTC"))
    )
