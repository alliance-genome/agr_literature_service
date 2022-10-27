"""
reference_mod_md5sum_model.py
===============
"""


from datetime import datetime
import pytz
from sqlalchemy import (Column, ForeignKey, Integer, String, DateTime)
from sqlalchemy.orm import relationship
from agr_literature_service.api.database.base import Base
from sqlalchemy.schema import Index


class ReferenceModMd5sumModel(Base):
    __tablename__ = "reference_mod_md5sum"

    reference_mod_md5sum_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    reference_id = Column(
        Integer,
        ForeignKey("reference.reference_id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )

    reference = relationship(
        "ReferenceModel",
        back_populates="reference_mod_md5sum"
    )

    mod_id = Column(
        Integer,
        ForeignKey("mod.mod_id", ondelete="CASCADE"),
        index=True,
        nullable=True,
    )

    mod = relationship(
        "ModModel",
        back_populates="reference_mod_md5sum"
    )

    md5sum = Column(
        String(),
        unique=True,
        nullable=False
    )

    date_updated = Column(
        DateTime,
        default=lambda: datetime.now(tz=pytz.timezone("UTC")),
        onupdate=lambda: datetime.now(tz=pytz.timezone("UTC")),
        nullable=False
    )

    __table_args__ = (
        Index(
            "uix_reference_id_mod_id",
            "reference_id",
            "mod_id",
            unique=True,
            postgresql_where=mod_id.isnot(None)
        ),
        Index(
            "uix_reference_id",
            "reference_id",
            unique=True,
            postgresql_where=mod_id.is_(None)
        ),
    )
