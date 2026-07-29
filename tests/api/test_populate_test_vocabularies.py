from agr_literature_service.lit_processing.tests.vocabulary_populate_load import (
    populate_test_vocabularies,
)
from agr_literature_service.api.crud import vocabulary_crud
from agr_literature_service.api.crud import vocabulary_seed_data as sd
from ..fixtures import db  # noqa


def test_populate_is_idempotent_and_complete(db):  # noqa
    populate_test_vocabularies(db)
    populate_test_vocabularies(db)  # second call must not duplicate
    lab = vocabulary_crud.get_vocabulary(db, sd.LAB_POSITION_VOCAB)
    ppr = vocabulary_crud.get_vocabulary(db, sd.PERSON_LINEAGE_VOCAB)
    assert len(lab) == 15
    assert len(ppr) == 11
    assert {t["label"] for t in ppr} == set(sd.PERSON_LINEAGE_TERMS)
