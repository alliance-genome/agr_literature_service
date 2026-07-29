"""Canonical seed data for the ABC controlled vocabularies (SCRUM-6311).

Single source of truth shared by the alembic seed migration, the test-seed
helper, and the CRUD symmetry check. Adding terms AFTER the seed migration must
be done via CRUD or a new migration — editing these lists only affects a
brand-new database that runs the seed migration fresh.
"""
from typing import Dict, List, Set

SEED_USER_ID: str = "default_user"

LAB_POSITION_VOCAB: str = "lab_position"
PERSON_LINEAGE_VOCAB: str = "person_person_relationship"

# Laboratory Role — human-readable labels (14). PI status is encoded by the
# ``is_pi`` timestamp on laboratory_person, so there is no PI role term here.
LAB_POSITION_TERMS: List[str] = [
    "Group Leader",
    "Senior Scientist",
    "MD/Veterinarian",
    "Animal Care Staff",
    "Research Associate/Assistant",
    "Lab Manager",
    "Post-Doc",
    "Graduate Student",
    "Undergraduate Student",
    "Technician",
    "Programmer/Bioinformatician",
    "Administrative Staff",
    "Lab Member",
    "Professional Biocurator",
]

# Person Lineage — retired slug -> human-readable label (11). The slug is used
# ONLY for the one-time migration backfill; it is not persisted anywhere.
PERSON_LINEAGE_SLUG_TO_LABEL: Dict[str, str] = {
    "phd_supervisor_of": "PhD Supervisor of",
    "postdoc_supervisor_of": "Postdoc Supervisor of",
    "masters_supervisor_of": "Master's Supervisor of",
    "undergrad_supervisor_of": "Undergraduate Supervisor of",
    "highschool_supervisor_of": "High School Supervisor of",
    "sabbatical_supervisor_of": "Sabbatical Supervisor of",
    "lab_visitor_supervisor_of": "Lab Visitor Supervisor of",
    "research_staff_supervisor_of": "Research Staff Supervisor of",
    "assistant_professor_supervisor_of": "Assistant Professor Supervisor of",
    "unknown_supervisor_of": "Unknown Role Supervisor of",
    "collaborator_of": "Collaborator of",
}

PERSON_LINEAGE_TERMS: List[str] = list(PERSON_LINEAGE_SLUG_TO_LABEL.values())

# Relationship labels that are NOT directional: (A,B) and (B,A) are the same fact.
SYMMETRIC_RELATIONSHIP_NAMES: Set[str] = {"Collaborator of"}
