"""
editor_model.py
===============
"""


from typing import Dict

from sqlalchemy import Column, ForeignKey, Integer, String
# from sqlalchemy.orm import relationship

from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning
from agr_literature_service.api.models.audited_model import AuditedModel

enable_versioning()


class EditorModel(Base, AuditedModel):
    __tablename__ = "editor"
    __versioned__: Dict = {}

    editor_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    resource_id = Column(
        Integer,
        ForeignKey("resource.resource_id",
                   ondelete="CASCADE"),
        index=True
    )

    """
    resource = relationship(
        "ResourceModel",
        back_populates="editor"
    )
    """

    orcid = Column(
        String(),
        index=True,
        nullable=True
    )

    order = Column(
        Integer,
        nullable=True
    )

    name = Column(
        String(),
        unique=False,
        nullable=True
    )

    first_name = Column(
        String(),
        unique=False,
        nullable=True
    )

    last_name = Column(
        String(),
        unique=False,
        nullable=True
    )

    def __str__(self):
        """
        Overwrite the default output.
        """
        return f"{self.name} 1st({self.first_name}) last({self.last_name}) order({self.order})"
