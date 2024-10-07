"""
resource_descriptor_models.py
==============================
"""


from sqlalchemy import ARRAY, Column, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning


enable_versioning()


class ResourceDescriptorPageModel(Base):
    __tablename__ = "resource_descriptor_pages"

    resource_descriptor_pages_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    name = Column(
        String,
        unique=False,
        nullable=False
    )

    url = Column(
        String,
        unique=False,
        nullable=False
    )

    resource_descriptor_id = Column(
        Integer,
        ForeignKey("resource_descriptors.resource_descriptor_id",
                   ondelete="CASCADE"),
        index=True
    )

    resource_descriptor = relationship(
        "ResourceDescriptorModel",
        back_populates="pages"
    )


class ResourceDescriptorModel(Base):
    __tablename__ = "resource_descriptors"

    resource_descriptor_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    pages = relationship(
        "ResourceDescriptorPageModel",
        lazy="joined",
        back_populates="resource_descriptor",
        cascade="all, delete, delete-orphan"
    )

    db_prefix = Column(
        String,
        nullable=False,
        unique=True
    )

    name = Column(
        String(),
        unique=False,
        nullable=True
    )

    aliases: Column = Column(
        ARRAY(String()),
        nullable=True
    )

    example_gid = Column(
        String,
        nullable=True
    )

    gid_pattern = Column(
        String,
        nullable=True
    )

    default_url = Column(
        String(),
        unique=False,
        nullable=True
    )
