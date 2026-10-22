from typing import Dict, List

from sqlalchemy import BigInteger, Column, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import relationship, mapped_column, Mapped
from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning
from agr_literature_service.api.models.audited_model import AuditedModel
from agr_literature_service.api.models.ontology_term_model import ontology_curie, register_curie_columns

enable_versioning()


class CurationStatusModel(Base, AuditedModel):
    __tablename__ = 'curation_status'
    # SCRUM-6518: curation_status had no history at all; the spike asked for it
    # on the base row as well as on the source associations.
    __versioned__: Dict = {}

    curation_status_id: Mapped[int] = mapped_column(
        'id',
        primary_key=True,
        autoincrement=True
    )

    # SCRUM-6720: topic, curation_status and curation_tag are ATP ontologyterm ids; the attributes
    # are their curies. curation_status is stored as curation_status_term_id: curation_status_id is
    # this table's key attribute and, on the source associations, the FK to this table.
    topic_id = Column(BigInteger, ForeignKey("ontologyterm.id"), index=True, nullable=False)
    topic = ontology_curie(topic_id)

    reference_id = Column(
        Integer,
        ForeignKey("reference.id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )

    reference = relationship(
        "ReferenceModel",
        foreign_keys="CurationStatusModel.reference_id"
    )

    mod_id = Column(
        Integer,
        ForeignKey("mod.id", ondelete="CASCADE"),
        index=True,
        nullable=False
    )

    mod = relationship(
        "ModModel",
        foreign_keys="CurationStatusModel.mod_id"
    )

    curation_status_term_id = Column(BigInteger, ForeignKey("ontologyterm.id"), nullable=True)
    curation_status = ontology_curie(curation_status_term_id)

    curation_tag_id = Column(BigInteger, ForeignKey("ontologyterm.id"), nullable=True)
    curation_tag = ontology_curie(curation_tag_id)

    note = Column(
        String,
        nullable=True
    )

    # Optional per-source reports against this row. Independent of the values
    # above: adding, updating or deleting one never modifies this row.
    source_associations: Mapped[List["CurationStatusSourceAssociationModel"]] = relationship(  # type: ignore  # noqa
        back_populates="curation_status_row",
        cascade="all, delete-orphan"
    )

    __table_args__ = (
        UniqueConstraint(
            'topic_id', 'reference_id', 'mod_id',
            name='curation_status_unique'),
    )

    def __str__(self):
        """
        Overwrite the default output.
        """
        return f"topic:{self.topic} mod:{self.mod} ref:{self.reference}"


register_curie_columns(CurationStatusModel, {
    "topic": ("topic_id", ("ATPTerm",)),
    "curation_status": ("curation_status_term_id", ("ATPTerm",)),
    "curation_tag": ("curation_tag_id", ("ATPTerm",)),
})
