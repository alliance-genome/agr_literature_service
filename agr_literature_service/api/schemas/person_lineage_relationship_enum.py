from enum import Enum


class PersonPersonRole(str, Enum):
    phd_supervisor_of = "phd_supervisor_of"
    postdoc_supervisor_of = "postdoc_supervisor_of"
    masters_supervisor_of = "masters_supervisor_of"
    undergrad_supervisor_of = "undergrad_supervisor_of"
    highschool_supervisor_of = "highschool_supervisor_of"
    sabbatical_supervisor_of = "sabbatical_supervisor_of"
    lab_visitor_supervisor_of = "lab_visitor_supervisor_of"
    research_staff_supervisor_of = "research_staff_supervisor_of"
    assistant_professor_supervisor_of = "assistant_professor_supervisor_of"
    unknown_supervisor_of = "unknown_supervisor_of"
    collaborator_of = "collaborator_of"


# Relationships that are NOT directional: (A, B) and (B, A) denote the same fact.
# For these the canonical person_lineage stores the pair in normalized id order so
# the unique constraint dedups regardless of submitted direction.
SYMMETRIC_RELATIONSHIPS = {PersonPersonRole.collaborator_of.value}
