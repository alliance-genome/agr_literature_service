"""
curation_status_source_association_model.py
===========================================

SCRUM-6518. One row per (curation_status, tag_source): what THAT source
reported for this (reference, MOD, topic). Associations are independent of the
curation_status row they hang off - their values may disagree with it and with
each other, and nothing is reconciled at write time.
"""

from typing import Dict

from sqlalchemy import Column, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import relationship

from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning
from agr_literature_service.api.models.audited_model import AuditedModel

enable_versioning()


class CurationStatusSourceAssociationModel(AuditedModel, Base):
    __tablename__ = "curation_status_source_association"
    __versioned__: Dict = {}

    curation_status_source_association_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    curation_status_id = Column(
        Integer,
        ForeignKey("curation_status.curation_status_id", ondelete="CASCADE"),
        index=True,
        nullable=False
    )

    # NOT named `curation_status`: that name belongs to this table's own
    # reported-value column below.
    curation_status_row = relationship(
        "CurationStatusModel",
        foreign_keys="CurationStatusSourceAssociationModel.curation_status_id",
        back_populates="source_associations"
    )

    # ON DELETE CASCADE matches the existing topic_entity_tag -> tag_source FK.
    # Deleting a source therefore drops its attributions; sources are not
    # deleted in normal operation.
    tag_source_id = Column(
        Integer,
        ForeignKey("tag_source.tag_source_id", ondelete="CASCADE"),
        index=True,
        nullable=False
    )

    tag_source = relationship(
        "TagSourceModel",
        foreign_keys="CurationStatusSourceAssociationModel.tag_source_id"
    )

    # The values THIS source reports. All nullable: a source may assert only a
    # note, or only a status.
    curation_status = Column(
        String,
        nullable=True
    )

    curation_tag = Column(
        String,
        nullable=True
    )

    note = Column(
        String,
        nullable=True
    )

    __table_args__ = (
        UniqueConstraint(
            'curation_status_id', 'tag_source_id',
            name='curation_status_source_association_unique'),
    )

    def __str__(self):
        return (f"curation_status_id:{self.curation_status_id} "
                f"tag_source_id:{self.tag_source_id} status:{self.curation_status}")
