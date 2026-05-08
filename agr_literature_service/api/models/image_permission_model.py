"""
image_permission_model.py
=========================
"""

from typing import Dict

from sqlalchemy import Boolean, CheckConstraint, Column, ForeignKey, Index, Integer, String, TEXT, func
from sqlalchemy.orm import relationship

from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning
from agr_literature_service.api.models.audited_model import AuditedModel

enable_versioning()


class ImagePermissionModel(Base, AuditedModel):
    __versioned__: Dict = {}
    __tablename__ = "image_permission"

    image_permission_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    name = Column(
        String(),
        unique=True,
        nullable=False,
        index=True
    )

    permission_text = Column(
        TEXT(),
        nullable=False
    )

    permission_url = Column(
        String(),
        nullable=True
    )

    can_display_images = Column(
        Boolean,
        nullable=False,
        default=False,
        server_default="false"
    )

    resource_image_permissions = relationship(
        "ResourceImagePermissionModel",
        back_populates="image_permission",
        cascade="all, delete, delete-orphan",
        passive_deletes=True
    )


class ResourceImagePermissionModel(Base, AuditedModel):
    __versioned__: Dict = {}
    __tablename__ = "resource_image_permission"
    __table_args__ = (
        CheckConstraint(
            "end_year IS NULL OR start_year IS NULL OR end_year >= start_year",
            name="ck_resource_image_permission_year_range"
        ),
        CheckConstraint(
            "(start_year IS NULL OR start_year >= 0) AND (end_year IS NULL OR end_year >= 0)",
            name="ck_resource_image_permission_year_nonnegative"
        ),
    )

    resource_image_permission_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    resource_id = Column(
        Integer,
        ForeignKey("resource.resource_id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )

    image_permission_id = Column(
        Integer,
        ForeignKey("image_permission.image_permission_id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )

    start_year = Column(
        Integer,
        nullable=True
    )

    end_year = Column(
        Integer,
        nullable=True
    )

    notes = Column(
        TEXT(),
        nullable=True
    )

    resource = relationship(
        "ResourceModel",
        back_populates="resource_image_permissions"
    )

    image_permission = relationship(
        "ImagePermissionModel",
        back_populates="resource_image_permissions"
    )


Index(
    "uq_resource_image_permission_range",
    ResourceImagePermissionModel.resource_id,
    ResourceImagePermissionModel.image_permission_id,
    func.coalesce(ResourceImagePermissionModel.start_year, -1),
    func.coalesce(ResourceImagePermissionModel.end_year, -1),
    unique=True
)
