"""
topic_entity_tag_model.py
==================
"""

from typing import Dict, List

from sqlalchemy import Column, ForeignKey, Integer, String, and_, CheckConstraint, UniqueConstraint, Boolean, or_, Table
from sqlalchemy.orm import relationship, Mapped

from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning
from agr_literature_service.api.models.audited_model import AuditedModel

enable_versioning()


topic_entity_tag_validation = Table(
    "topic_entity_tag_validation",
    Base.metadata,
    Column(
        "validated_topic_entity_tag_id",
        ForeignKey("topic_entity_tag.topic_entity_tag_id", ondelete='CASCADE'),
        index=True,
        primary_key=True
    ),

    Column(
        "validating_topic_entity_tag_id",
        ForeignKey("topic_entity_tag.topic_entity_tag_id", ondelete='CASCADE'),
        index=True,
        primary_key=True,
    ),

    UniqueConstraint(
        'validated_topic_entity_tag_id', 'validating_topic_entity_tag_id', name='validation_unique'),
)


class TopicEntityTagModel(AuditedModel, Base):
    __tablename__ = "topic_entity_tag"
    __versioned__ = {
        'exclude': ['validated_by']
    }

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

    topic_entity_tag_source = relationship(
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

    entity_id_validation = Column(
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
        nullable=True,
        unique=False
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

    novel_topic_data = Column(
        Boolean,
        nullable=False,
        unique=False,
        default=False,
        server_default='false'
    )

    validated_by = relationship(
        "TopicEntityTagModel",
        secondary=topic_entity_tag_validation,
        primaryjoin=topic_entity_tag_validation.c.validated_topic_entity_tag_id == topic_entity_tag_id,
        secondaryjoin=topic_entity_tag_validation.c.validating_topic_entity_tag_id == topic_entity_tag_id,
        foreign_keys=[topic_entity_tag_validation.c.validated_topic_entity_tag_id,
                      topic_entity_tag_validation.c.validating_topic_entity_tag_id]
    )

    validation_by_author = Column(
        String(),
        nullable=True,
        unique=False
    )

    validation_by_professional_biocurator = Column(
        String(),
        nullable=True,
        unique=False
    )

    # Add relationship to Dataset
    dataset_entries: Mapped[List["DatasetEntryModel"]] = relationship(back_populates="supporting_topic_entity_tag")

    def __str__(self):
        return f"id:{self.topic_entity_tag_id}\ttopic:{self.topic}" \
               f"\n-\tval_auth:{self.validation_by_author}\tval_pb:{self.validation_by_professional_biocurator}" \
               f"\n-\teny_type:{self.entity_type}\t entity:{self.entity}"

    __table_args__ = (
        CheckConstraint(
            or_(
                and_(entity_type.isnot(None), entity.isnot(None), entity_id_validation.isnot(None)),
                and_(entity_type.is_(None), entity.is_(None), entity_id_validation.is_(None))
            ),
            name="valid_entity_type_dependencies"
        ),
    )


class TopicEntityTagSourceModel(AuditedModel, Base):
    __tablename__ = "topic_entity_tag_source"
    __versioned__: Dict = {}

    topic_entity_tag_source_id = Column(
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
        foreign_keys="TopicEntityTagSourceModel.secondary_data_provider_id"
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
            name='topic_entity_tag_source_unique'),
    )
