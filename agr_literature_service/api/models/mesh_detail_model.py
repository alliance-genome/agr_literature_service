"""
mesh_detail_model.py
====================
"""

from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning
from typing import Dict

enable_versioning()
class MeshDetailModel(Base):
    __tablename__ = "mesh_detail"
    __bind_key__ = 'lit'
    __table_args__ = {"schema": "lit"}
    __versioned__: Dict = {}

    mesh_detail_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    reference_id = Column(
        Integer,
        ForeignKey("lit.reference.reference_id",
                   ondelete="CASCADE"),
        index=True
    )

    reference = relationship(
        "ReferenceModel",
        back_populates="mesh_term"
    )

    heading_term = Column(
        String,
        unique=False,
        nullable=False
    )

    qualifier_term = Column(
        String,
        unique=False,
        nullable=True
    )

    def __str__(self):
        """
        Overwrite the default output.
        """
        return "ht={}, qt={}".format(self.heading_term, self.qualifier_term)
