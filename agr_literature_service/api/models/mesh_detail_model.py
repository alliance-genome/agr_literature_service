"""
mesh_detail_model.py
====================
"""

from sqlalchemy import BigInteger, Column, ForeignKey, String
from sqlalchemy.orm import relationship

from agr_literature_service.api.database.base import Base


class MeshDetailModel(Base):
    __tablename__ = "mesh_detail"

    mesh_detail_id = Column(
        'id',
        BigInteger,
        primary_key=True,
        autoincrement=True
    )

    reference_id = Column(
        BigInteger,
        ForeignKey("reference.id",
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
