from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.orm import relationship, mapped_column, Mapped
from agr_literature_service.api.database.base import Base
from agr_literature_service.api.models.audited_model import AuditedModel


class CurationStatusModel(Base, AuditedModel):
    __tablename__ = 'curation_status'

    curation_status_id: Mapped[int] = mapped_column(
        primary_key=True,
        autoincrement=True
    )

    topic = Column(
        String,
        index=True,
        nullable=False
    )

    reference_id = Column(
        Integer,
        ForeignKey("reference.reference_id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )

    reference = relationship(
        "ReferenceModel",
        foreign_keys="CurationStatusModel.reference_id"
    )

    mod_id = Column(
        Integer,
        ForeignKey("mod.mod_id", ondelete="CASCADE"),
        index=True,
        nullable=False
    )

    mod = relationship(
        "ModModel",
        foreign_keys="CurationStatusModel.mod_id"
    )

    curation_status = Column(
        String,
        nullable=True
    )

    controlled_note = Column(
        String,
        nullable=True
    )

    note = Column(
        String,
        nullable=True
    )

    def __str__(self):
        """
        Overwrite the default output.
        """
        return f"topic:{self.topic} mod:{self.mod} ref:{self.reference}"
