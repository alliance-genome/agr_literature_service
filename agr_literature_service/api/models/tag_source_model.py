"""
tag_source_model.py
===================

A source of an assertion. Shared by topic_entity_tag and by
curation_status_source_association (SCRUM-6518); formerly
topic_entity_tag_source.
"""

from typing import Dict

from sqlalchemy import Column, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import relationship

from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning
from agr_literature_service.api.models.audited_model import AuditedModel

enable_versioning()


class TagSourceModel(AuditedModel, Base):
    __tablename__ = "tag_source"
    __versioned__: Dict = {}

    tag_source_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    data_provider = Column(
        String(),
        unique=False,
        nullable=False,
        index=True
    )

    secondary_data_provider_id = Column(
        Integer,
        ForeignKey("mod.mod_id", ondelete="CASCADE"),
        index=True,
        nullable=False
    )

    secondary_data_provider = relationship(
        "ModModel",
        foreign_keys="TagSourceModel.secondary_data_provider_id"
    )

    source_evidence_assertion = Column(
        String(),
        unique=False,
        nullable=False,
        index=True
    )

    source_method = Column(
        String(),
        unique=False,
        nullable=False,
        index=True
    )

    validation_type = Column(
        String(),
        unique=False,
        nullable=True,
        index=True
    )

    description = Column(
        String(),
        unique=False,
        nullable=True
    )

    __table_args__ = (
        UniqueConstraint(
            'source_evidence_assertion', 'source_method', 'data_provider', 'secondary_data_provider_id',
            name='tag_source_unique'),
    )
