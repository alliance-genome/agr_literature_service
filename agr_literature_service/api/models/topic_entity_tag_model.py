"""
topic_entity_tag_model.py
==================
"""


from typing import Dict

from sqlalchemy import Column, ForeignKey, Integer, String, and_, CheckConstraint, UniqueConstraint, Boolean, or_, Index
from sqlalchemy.orm import relationship

from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning
from agr_literature_service.api.models.audited_model import AuditedModel

enable_versioning()


class TopicEntityTagModel(AuditedModel, Base):
    __tablename__ = "topic_entity_tag"
    __versioned__: Dict = {}

    topic_entity_tag_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    reference_id = Column(
        Integer,
        ForeignKey("reference.reference_id", ondelete="CASCADE"),
        index=True,
        nullable=False
    )

    reference = relationship(
        "ReferenceModel",
        foreign_keys="TopicEntityTagModel.reference_id",
        back_populates="topic_entity_tags"
    )

    topic_entity_tag_source_id = Column(
        Integer,
        ForeignKey("topic_entity_tag_source.topic_entity_tag_source_id", ondelete="CASCADE"),
        index=True,
        nullable=False
    )

    topic_entity_type_source = relationship(
        "TopicEntityTagSourceModel",
        foreign_keys="TopicEntityTagModel.topic_entity_tag_source_id"
    )

    # Obtained from A-Team ontology node term-id
    topic = Column(
        String(),
        unique=False,
        nullable=False
    )

    # Obtained from A-Team ontology node term-id
    entity_type = Column(
        String(),
        unique=False,
        nullable=True
    )

    entity = Column(
        String(),
        unique=False,
        nullable=True
    )

    entity_source = Column(
        String(),
        unique=False,
        nullable=True
    )

    entity_published_as = Column(
        String(),
        unique=False,
        nullable=True
    )

    species = Column(
        String(),
        unique=False,
        nullable=True
    )

    display_tag = Column(
        String(),
        nullable=True
    )

    negated = Column(
        Boolean,
        nullable=False,
        unique=False,
        default=False
    )

    confidence_level = Column(
        String(),
        nullable=True,
        unique=False
    )

    note = Column(
        String(),
        nullable=True,
        unique=False
    )

    __table_args__ = (
        CheckConstraint(
            or_(
                and_(entity_type.isnot(None), entity.isnot(None), entity_source.isnot(None), species.isnot(None)),
                and_(entity_type.is_(None), entity.is_(None), entity_source.is_(None)),
                and_(entity_type.isnot(None), entity.is_(None), entity_source.is_(None), species.isnot(None),
                     negated.is_(True))
            ),
            name="entity_entity_source_and_species_not_null_when_entity_type_provided"
        ),
        Index(
            'ix_unique_topic_tag_with_species',
            'reference_id', 'topic', 'species', 'topic_entity_tag_source_id', 'created_by',
            unique=True,
            postgresql_where=and_(entity_type.is_(None), species.isnot(None))),
        Index(
            'ix_unique_topic_tag_without_species',
            'reference_id', 'topic', 'topic_entity_tag_source_id', 'created_by',
            unique=True,
            postgresql_where=and_(entity_type.is_(None), species.is_(None))),
        Index(
            'ix_unique_entity_tag',
            'reference_id', 'topic', 'entity_type', 'entity', 'entity_source', 'species',
            'entity_published_as', 'topic_entity_tag_source_id', 'created_by',
            unique=True,
            postgresql_where=entity_type.isnot(None))
    )


class TopicEntityTagSourceModel(AuditedModel, Base):
    __tablename__ = "topic_entity_tag_source"
    __versioned__: Dict = {}

    topic_entity_tag_source_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    mod_id = Column(
        Integer,
        ForeignKey("mod.mod_id", ondelete="CASCADE"),
        index=True,
        nullable=False
    )

    mod = relationship(
        "ModModel",
        foreign_keys="TopicEntityTagSourceModel.mod_id"
    )

    source_type = Column(
        String(),
        unique=False,
        nullable=False
    )

    source_details = Column(
        String(),
        unique=False,
        nullable=False
    )

    evidence = Column(
        String(),
        unique=False,
        nullable=False
    )

    __table_args__ = (
        UniqueConstraint(
            'source_type', 'source_details', 'mod_id', name='topic_entity_tag_source_unique'),
    )
