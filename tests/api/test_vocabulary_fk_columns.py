from sqlalchemy import inspect
from agr_literature_service.api.models import (
    LaboratoryPersonModel, PersonLineageModel, PersonLineageSubmissionModel,
)


def _cols(model):
    return {c.key for c in inspect(model).columns}


def test_fk_columns_present_and_old_string_columns_gone():
    lp = _cols(LaboratoryPersonModel)
    assert "lab_position_vocab_term_abc_id" in lp
    assert "lab_position" not in lp

    pl = _cols(PersonLineageModel)
    assert "relationship_vocab_term_abc_id" in pl
    assert "relationship" not in pl

    ps = _cols(PersonLineageSubmissionModel)
    assert "relationship_vocab_term_abc_id" in ps
    assert "relationship" not in ps
