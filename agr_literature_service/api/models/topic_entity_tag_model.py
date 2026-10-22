"""
topic_entity_tag_model.py
==================
"""

from typing import List

from sqlalchemy import BigInteger, Column, ForeignKey, Integer, String, Float, \
    and_, CheckConstraint, UniqueConstraint, Boolean, or_, Table
from sqlalchemy.orm import relationship, Mapped

from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning
from agr_literature_service.api.models.audited_model import AuditedModel
from agr_literature_service.api.models.biological_entity_model import register_entity_column
from agr_literature_service.api.models.ontology_term_model import ontology_curie, register_curie_columns
# TagSourceModel lived here until SCRUM-6518 made the source table shared with
# curation_status. Imported so the "TagSourceModel" relationship target below
# resolves wherever this module is imported on its own.
from agr_literature_service.api.models.tag_source_model import TagSourceModel  # noqa: F401

enable_versioning()


topic_entity_tag_validation = Table(
    "topic_entity_tag_validation",
    Base.metadata,
    Column(
        "validated_topic_entity_tag_id",
        ForeignKey("topic_entity_tag.id", ondelete='CASCADE'),
        index=True,
        primary_key=True
    ),

    Column(
        "validating_topic_entity_tag_id",
        ForeignKey("topic_entity_tag.id", ondelete='CASCADE'),
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
        'id',
        Integer,
        primary_key=True,
        autoincrement=True
    )

    reference_id = Column(
        Integer,
        ForeignKey("reference.id", ondelete="CASCADE"),
        index=True,
        nullable=False
    )

    reference = relationship(
        "ReferenceModel",
        foreign_keys="TopicEntityTagModel.reference_id",
        back_populates="topic_entity_tags"
    )

    tag_source_id = Column(
        Integer,
        ForeignKey("tag_source.id", ondelete="CASCADE"),
        index=True,
        nullable=False
    )

    tag_source = relationship(
        "TagSourceModel",
        foreign_keys="TopicEntityTagModel.tag_source_id"
    )

    # SCRUM-6720: the ontology term columns hold A-team ontologyterm ids; topic,
    # entity_type, species, display_tag, data_novelty and data_context are their
    # curies (ontology_term_model.ontology_curie)
    topic_id = Column(BigInteger, ForeignKey("ontologyterm.id"), index=True, nullable=False)
    topic = ontology_curie(topic_id)

    entity_type_id = Column(BigInteger, ForeignKey("ontologyterm.id"), index=True, nullable=True)
    entity_type = ontology_curie(entity_type_id)

    entity = Column(
        String(),
        unique=False,
        nullable=True,
        index=True
    )

    # SCRUM-6720 Phase 2: A-team's biologicalentity of entity, when it has one (set from entity on
    # every write; NULL for entities A-team does not hold). entity stays the source of truth.
    entity_id = Column(BigInteger, ForeignKey("biologicalentity.id", ondelete="SET NULL"), index=True, nullable=True)

    entity_id_validation = Column(
        String(),
        unique=False,
        nullable=True
    )

    species_id = Column(BigInteger, ForeignKey("ontologyterm.id"), index=True, nullable=True)
    species = ontology_curie(species_id)

    display_tag_id = Column(BigInteger, ForeignKey("ontologyterm.id"), nullable=True)
    display_tag = ontology_curie(display_tag_id)

    negated = Column(
        Boolean,
        nullable=True,
        unique=False,
        index=True
    )

    confidence_score = Column(
        Float(),
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

    data_novelty_id = Column(BigInteger, ForeignKey("ontologyterm.id"), index=True, nullable=False)
    data_novelty = ontology_curie(data_novelty_id)

    # SCRUM-5697. What kind of data the tag represents, as an ATP term drawn
    # from a hierarchy rather than a flat set: the ATP:0000323 "data context"
    # root over ATP:0000324 mentioned data (ATP:0000360 background information,
    # ATP:0000325 experimentally studied data) and ATP:0000326 marker data
    # (ATP:0000328 expression marker, ATP:0000327 genetic marker). The four
    # leaves are the editor's curation choices; WB topic tags store the root.
    data_context_id = Column(BigInteger, ForeignKey("ontologyterm.id"), index=True, nullable=False)
    data_context = ontology_curie(data_context_id)

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

    ml_model_id = Column(
        Integer,
        ForeignKey("ml_model.id", ondelete="SET NULL"),
        index=True,
        nullable=True
    )

    ml_model = relationship(
        "MLModel",
        foreign_keys="TopicEntityTagModel.ml_model_id"
    )

    # Add relationship to Dataset
    dataset_entries: Mapped[List["DatasetEntryModel"]] = relationship(back_populates="supporting_topic_entity_tag")  # type: ignore  # noqa

    def __str__(self):
        return f"id:{self.topic_entity_tag_id}\ttopic:{self.topic}" \
               f"\n-\tval_auth:{self.validation_by_author}\tval_pb:{self.validation_by_professional_biocurator}" \
               f"\n-\teny_type:{self.entity_type}\t entity:{self.entity}"

    __table_args__ = (
        CheckConstraint(
            or_(
                and_(entity_type_id.isnot(None), entity.isnot(None), entity_id_validation.isnot(None)),
                and_(entity_type_id.is_(None), entity.is_(None), entity_id_validation.is_(None))
            ),
            name="valid_entity_type_dependencies"
        ),
    )


register_curie_columns(TopicEntityTagModel, {
    "topic": ("topic_id", ("ATPTerm",)),
    "entity_type": ("entity_type_id", ("ATPTerm",)),
    "species": ("species_id", ("NCBITaxonTerm",)),
    "display_tag": ("display_tag_id", ("ATPTerm",)),
    "data_novelty": ("data_novelty_id", ("ATPTerm",)),
    "data_context": ("data_context_id", ("ATPTerm",)),
})
register_entity_column(TopicEntityTagModel)
