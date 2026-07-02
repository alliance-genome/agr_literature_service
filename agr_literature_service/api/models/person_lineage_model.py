from typing import Dict
from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import relationship as orm_relationship
from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning
from agr_literature_service.api.models.audited_model import AuditedModel

enable_versioning()


class PersonLineageModel(Base, AuditedModel):
    """A validated person-to-person relationship (canonical fact).

    Keyed on the two resolved person ids; there is exactly one row per
    (person_subject_id, person_object_id, relationship). Display names come from
    joining the person table, so no name columns are stored here.
    """
    __tablename__ = "person_lineage"
    __versioned__: Dict = {}

    person_lineage_id = Column(Integer, primary_key=True, autoincrement=True)

    person_subject_id = Column(
        Integer,
        ForeignKey("person.person_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    person_object_id = Column(
        Integer,
        ForeignKey("person.person_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # Controlled vocabulary enforced by the API (PersonPersonRole).
    relationship = Column(String(), nullable=False)

    start_date = Column(DateTime, nullable=True)
    end_date = Column(DateTime, nullable=True)

    person_subject_obj = orm_relationship("PersonModel", foreign_keys=[person_subject_id])
    person_object_obj = orm_relationship("PersonModel", foreign_keys=[person_object_id])
    submissions = orm_relationship("PersonLineageSubmissionModel", back_populates="canonical")

    @property
    def person_subject_curie(self):
        """Convenience for serializers — the curie of the resolved person_subject."""
        return self.person_subject_obj.curie if self.person_subject_obj else None

    @property
    def person_object_curie(self):
        """Convenience for serializers — the curie of the resolved person_object."""
        return self.person_object_obj.curie if self.person_object_obj else None

    @property
    def person_subject_name(self):
        """Convenience for serializers — display name of the resolved person_subject."""
        return self.person_subject_obj.display_name if self.person_subject_obj else None

    @property
    def person_object_name(self):
        """Convenience for serializers — display name of the resolved person_object."""
        return self.person_object_obj.display_name if self.person_object_obj else None

    __table_args__ = (
        UniqueConstraint(
            "person_subject_id", "person_object_id", "relationship",
            name="uq_person_lineage_person_ids_relationship",
        ),
    )

    def __str__(self) -> str:
        return f"person_lineage({self.person_subject_id} -[{self.relationship}]-> {self.person_object_id})"
