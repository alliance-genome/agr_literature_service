"""
cross_reference_model.py
========================
"""


from typing import Dict

# from sqlalchemy import ARRAY, Boolean, Column, ForeignKey, Integer, String, Index,\
#    Sequence
from sqlalchemy import ARRAY, Boolean, Column, ForeignKey, Integer, String, Index
from sqlalchemy.orm import relationship

from agr_literature_service.api.database.base import Base
from agr_literature_service.api.models.audited_model import AuditedModel
from agr_literature_service.api.database.versioning import enable_versioning


enable_versioning()


class CrossReferenceModel(Base, AuditedModel):
    __tablename__ = "cross_reference"
    __versioned__: Dict = {}

    cross_reference_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    # cross_reference_id = Column(
    #    Integer,
    #    Sequence('cross_reference_id_seq', start=0, increment=1),
    #    primary_key=True
    # )

    curie = Column(
        String(),
        nullable=False,
        index=True
    )

    curie_prefix = Column(
        String(),
        nullable=False
    )

    is_obsolete = Column(
        Boolean,
        unique=False,
        default=False
    )

    reference_id = Column(
        Integer,
        ForeignKey("reference.reference_id", ondelete="CASCADE"),
        index=True
    )

    reference = relationship(
        "ReferenceModel",
        back_populates="cross_reference"
    )

    resource_id = Column(
        Integer,
        ForeignKey("resource.resource_id"),
        index=True
    )

    resource = relationship(
        "ResourceModel",
        back_populates="cross_reference"
    )

    pages = Column(
        ARRAY(String()),
        nullable=True
    )

    __table_args__ = (
        Index('idx_curie_prefix_reference',
              'curie_prefix', 'reference_id',
              unique=True,
              postgresql_where=(is_obsolete.is_(False))),
        # Index('idx_curie_prefix_resource',
        #       'curie_prefix', 'resource_id',
        #       unique=True,
        #       postgresql_where=(is_obsolete.is_(False))),
        Index('idx_curie',
              'curie',
              unique=True,
              postgresql_where=(is_obsolete.is_(False)))
    )

    def __str__(self):
        """
        Overwrite the default output.
        """
        return "CrossReference: curie='{}' is_obsolete='{}' reference_id='{}', resource_id='{}' pages={}".\
            format(self.curie, self.is_obsolete, self.reference_id, self.resource_id, self.pages)
