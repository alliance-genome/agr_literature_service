"""
mod_corpus_association_model.py
===============
"""


from typing import Dict, Optional

from sqlalchemy import Column, ForeignKey, Integer, Enum, UniqueConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.sql.sqltypes import Boolean

from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning
from agr_literature_service.api.models.audited_model import AuditedModel
from agr_literature_service.api.schemas import ModCorpusSortSourceType

enable_versioning()


class ModCorpusAssociationModel(AuditedModel, Base):
    __tablename__ = "mod_corpus_association"
    __versioned__: Dict = {}

    mod_corpus_association_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    reference_id = Column(
        Integer,
        ForeignKey("reference.reference_id",
                   ondelete="CASCADE"),
        index=True
    )

    reference = relationship(
        "ReferenceModel",
        back_populates="mod_corpus_association"
    )

    mod_id = Column(
        Integer,
        ForeignKey("mod.mod_id", ondelete="CASCADE"),
        index=True
    )

    mod = relationship(
        "ModModel",
        lazy="joined"
    )

    corpus = Column(
        Boolean,
        nullable=True,
        default=None
    )

    mod_corpus_sort_source: Column = Column(
        Enum(ModCorpusSortSourceType),
        unique=False,
        nullable=False
    )

    __table_args__ = (UniqueConstraint('reference_id', 'mod_id', name='_mod_corpus_association_unique'),)
