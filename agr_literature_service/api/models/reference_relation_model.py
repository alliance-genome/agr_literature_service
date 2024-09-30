"""
reference_relation_model.py
==========================================
"""


from typing import Dict

from sqlalchemy import Column, Enum, ForeignKey, Integer, event, CheckConstraint, text
from sqlalchemy.orm import relationship

from agr_literature_service.api.database.base import Base
from agr_literature_service.api.schemas import ReferenceRelationType
from agr_literature_service.api.database.versioning import enable_versioning


enable_versioning()


class ReferenceRelationModel(Base):
    __tablename__ = "reference_relation"
    __versioned__: Dict = {}

    reference_relation_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    reference_id_from = Column(
        Integer,
        ForeignKey("reference.reference_id"),
        index=True,
        nullable=False
    )

    reference_from = relationship(
        "ReferenceModel",
        foreign_keys="ReferenceRelationModel.reference_id_from",
        back_populates="reference_relation_out"
    )

    reference_id_to = Column(
        Integer,
        ForeignKey("reference.reference_id"),
        index=True,
        nullable=False
    )

    reference_to = relationship(
        "ReferenceModel",
        foreign_keys="ReferenceRelationModel.reference_id_to",
        back_populates="reference_relation_in"
    )

    reference_relation_type = Column(
        Enum(ReferenceRelationType),
        unique=False,
        nullable=False
    )

    __table_args__ = (
        CheckConstraint('reference_id_from != reference_id_to', name='check_ref_ids_not_equal'),
    )


@event.listens_for(ReferenceRelationModel.__table__, 'after_create')
def receive_after_create(target, connection, **kw):
    connection.execute(text(
        "CREATE UNIQUE INDEX ix_reference_relation_least_greatest ON reference_relation ("
        "LEAST(reference_id_from, reference_id_to), GREATEST(reference_id_from, reference_id_to)"
        ");"
    ))
