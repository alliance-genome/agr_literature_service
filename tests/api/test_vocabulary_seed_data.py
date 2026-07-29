# tests/api/test_vocabulary_seed_data.py
from agr_literature_service.api.crud import vocabulary_seed_data as sd


def test_lab_position_has_15_terms_including_new_ones():
    assert len(sd.LAB_POSITION_TERMS) == 15
    assert "Professional Biocurator" in sd.LAB_POSITION_TERMS
    assert "Group Leader" in sd.LAB_POSITION_TERMS
    assert "Group leader" not in sd.LAB_POSITION_TERMS  # casing fix


def test_person_lineage_has_11_terms_and_slug_map():
    assert len(sd.PERSON_LINEAGE_SLUG_TO_LABEL) == 11
    assert sd.PERSON_LINEAGE_SLUG_TO_LABEL["collaborator_of"] == "Collaborator of"
    assert sd.PERSON_LINEAGE_SLUG_TO_LABEL["masters_supervisor_of"] == "Master's Supervisor of"
    assert sd.PERSON_LINEAGE_SLUG_TO_LABEL["undergrad_supervisor_of"] == "Undergraduate Supervisor of"
    assert set(sd.PERSON_LINEAGE_TERMS) == set(sd.PERSON_LINEAGE_SLUG_TO_LABEL.values())


def test_symmetric_names_and_keys():
    assert sd.SYMMETRIC_RELATIONSHIP_NAMES == {"Collaborator of"}
    assert sd.LAB_POSITION_VOCAB == "lab_position"
    assert sd.PERSON_LINEAGE_VOCAB == "person_person_relationship"
