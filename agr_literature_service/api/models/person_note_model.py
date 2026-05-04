from typing import Dict
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning
from agr_literature_service.api.models.audited_model import AuditedModel

enable_versioning()


class PersonNoteModel(Base, AuditedModel):
    __tablename__ = "person_note"
    __versioned__: Dict = {}

    person_note_id = Column(Integer, primary_key=True, autoincrement=True)

    person_id = Column(
        Integer,
        ForeignKey("person.person_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    person = relationship("PersonModel", back_populates="notes")

    note = Column(String(), nullable=False)

    def __str__(self) -> str:
        preview = (self.note or "")[:40]
        return f"PersonNote({self.person_note_id}) {preview}..."
