from typing import Dict
from sqlalchemy import Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import relationship as orm_relationship
from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning
from agr_literature_service.api.models.audited_model import AuditedModel

enable_versioning()


class PersonLineageSubmissionModel(Base, AuditedModel):
    """A submitted person-to-person relationship claim (curation working space).

    Submissions are append-only and kept for provenance even when duplicated.
    The two person links resolve independently (one side may be resolved while
    the other isn't); once both resolve and a curator validates, the submission
    is linked to a canonical person_lineage row.
    """
    __tablename__ = "person_lineage_submission"
    __versioned__: Dict = {}

    person_lineage_submission_id = Column(Integer, primary_key=True, autoincrement=True)

    # The claim — always required.
    person_subject_name = Column(String(), nullable=False)
    person_object_name = Column(String(), nullable=False)
    # Controlled vocabulary enforced by the API (PersonPersonRole).
    relationship = Column(String(), nullable=False)
    who_sent_this = Column(String(), nullable=False)

    # Resolution — set independently as a curator matches each name to a person.
    person_subject_id = Column(
        Integer,
        ForeignKey("person.person_id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    person_object_id = Column(
        Integer,
        ForeignKey("person.person_id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    start_date = Column(DateTime, nullable=True)
    end_date = Column(DateTime, nullable=True)

    # Controlled vocabulary enforced by the API (SubmissionStatus).
    status = Column(
        String(),
        nullable=False,
        default="pending",
        server_default="pending",
    )

    # Link to the canonical PPR once validated.
    person_lineage_id = Column(
        Integer,
        ForeignKey("person_lineage.person_lineage_id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    person_subject_obj = orm_relationship("PersonModel", foreign_keys=[person_subject_id])
    person_object_obj = orm_relationship("PersonModel", foreign_keys=[person_object_id])
    canonical = orm_relationship("PersonLineageModel", back_populates="submissions")

    @property
    def person_subject_curie(self):
        """Convenience for serializers — the curie of the resolved person_subject (if any)."""
        return self.person_subject_obj.curie if self.person_subject_obj else None

    @property
    def person_object_curie(self):
        """Convenience for serializers — the curie of the resolved person_object (if any)."""
        return self.person_object_obj.curie if self.person_object_obj else None

    def __str__(self) -> str:
        return (
            f"{self.person_subject_name} -[{self.relationship}]-> "
            f"{self.person_object_name} [{self.status}]"
        )
