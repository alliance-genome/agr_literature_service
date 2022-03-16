"""
mod_corpus_association_model.py
===============
"""


from datetime import datetime
from typing import Dict

import pytz
from sqlalchemy import ARRAY, Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from literature.database.base import Base


class ModCorpusAssociationModel(Base):
    __tablename__ = "mod_corpus_associations"
    __versioned__: Dict = {}

    mod_corpus_association_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    reference_id = Column(
        Integer,
        ForeignKey("references.reference_id",
                   ondelete="CASCADE"),
        index=True
    )

    reference = relationship(
        "ReferenceModel",
        back_populates="mod_corpus_associations"
    )

    mod_id = Column(
        Integer,
        ForeignKey("mods.mod_id",
                   ondelete="CASCADE"),
        index=True
    )

    mod = relationship(
        "ModModel",
        back_populates="mod_corpus_associations"
    )

    
    corpus = Column(
        Boolean,
        nullable=True,
        default=False
    )

    mod_corpus_sort_source = Column(
        Enum(ModCorpusSortSourceType),
        unique=False,
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
