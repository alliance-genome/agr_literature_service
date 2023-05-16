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
        nullable=False
    )

    qualifiers = relationship("TopicEntityTagQualifierModel", cascade="all,delete")

    sources = relationship("TopicEntityTagSourceModel", cascade="all,delete")

    __table_args__ = (
        CheckConstraint(
            or_(
                and_(entity_type.isnot(None), entity.isnot(None), entity_source.isnot(None)),
                and_(entity_type.is_(None), entity.is_(None), entity_source.is_(None))
            ),
            name="entity_and_entity_source_not_null_when_entity_type_provided"
        ),
        Index(
            'ix_unique_topic_tag',
            'reference_id', 'topic', 'species',
            unique=True,
            postgresql_where=entity_type.is_(None)),
        Index(
            'ix_unique_entity_tag',
            'reference_id', 'topic', 'entity_type', 'entity', 'entity_source', 'species',
            unique=True,
            postgresql_where=entity_type.isnot(None))
    )


class TopicEntityTagQualifierModel(AuditedModel, Base):
    __tablename__ = "topic_entity_tag_qualifier"
    __versioned__: Dict = {}

    topic_entity_tag_qualifier_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    topic_entity_tag_id = Column(
        Integer,
        ForeignKey("topic_entity_tag.topic_entity_tag_id", ondelete="CASCADE"),
        index=True,
        nullable=False
    )

    # Obtained from A-Team ontology qualifier
    qualifier = Column(
        String(),
        unique=False,
        nullable=False
    )

    qualifier_type = Column(
        String(),
        unique=False,
        nullable=False
    )

    mod_id = Column(
        Integer,
        ForeignKey("mod.mod_id", ondelete="CASCADE"),
        index=True,
        nullable=False
    )

    mod = relationship(
        "ModModel",
        foreign_keys="TopicEntityTagQualifierModel.mod_id"
    )


class TopicEntityTagSourceModel(AuditedModel, Base):
    __tablename__ = "topic_entity_tag_source"
    __versioned__: Dict = {}

    topic_entity_tag_source_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    topic_entity_tag_id = Column(
        Integer,
        ForeignKey("topic_entity_tag.topic_entity_tag_id", ondelete="CASCADE"),
        index=True,
        nullable=False
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

    source = Column(
        String(),
        unique=False,
        nullable=False
    )

    confidence_level = Column(
        String(),
        unique=False,
        nullable=True
    )

    validated = Column(
        Boolean(),
        unique=False,
        nullable=False
    )

    validation_type = Column(
        String(),
        unique=False,
        nullable=True
    )

    note = Column(
        String(),
        unique=False,
        nullable=True
    )

    __table_args__ = (
        UniqueConstraint(
            'source', 'topic_entity_tag_id',
            name='source_topic_entity_tag_unique'),
        CheckConstraint(
            or_(
                and_(validated.is_(True), validation_type.isnot(None)),
                and_(validated.is_(False), validation_type.is_(None)),
            ),
            name="validation_type_not_null_when_validation_provided")
    )
