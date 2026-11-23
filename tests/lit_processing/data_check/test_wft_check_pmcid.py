"""check_wft_in_progress retries FB gene extraction once the paper has a PMCID (SCRUM-6828).

FB gene extraction (FlyBERT) needs a PMCID. Papers often get theirs from the weekly
PubMed update or a DQM load weeks after the job failed, after the 6 week retry window,
so 37 FB papers stayed failed although a retry would now work.
"""
from datetime import datetime
from unittest.mock import patch

import pytest
from sqlalchemy import text

from agr_literature_service.api.models import (
    CrossReferenceModel, ModModel, ReferenceModel, WorkflowTagModel, WorkflowTransitionModel
)
from agr_literature_service.lit_processing.data_check.check_wft_in_progress import check_wft_in_progress
from agr_literature_service.lit_processing.tests.mod_populate_load import populate_test_mods
from ...fixtures import db, load_name_to_atp_and_relationships_mock  # noqa

GENE_FAILED = "ATP:0000216"
GENE_NEEDED = "ATP:0000220"
MAIN_FAILED = "ATP:0000187"
MAIN_IN_PROGRESS = "ATP:0000190"
FAILED_AT = datetime(2026, 7, 9, 4, 9)


def _paper(db, number, mod_abbreviation="FB", pmcid_added=None):  # noqa
    """A paper whose gene extraction failed at FAILED_AT, with a PMCID added at pmcid_added."""
    mod = db.query(ModModel).filter(ModModel.abbreviation == mod_abbreviation).one()
    reference = ReferenceModel(curie=f"AGRKB:10100050{number:04d}", category="research_article")
    db.add(reference)
    db.flush()
    db.add(WorkflowTagModel(reference=reference, mod=mod, workflow_tag_id=MAIN_FAILED))
    gene = WorkflowTagModel(reference=reference, mod=mod, workflow_tag_id=GENE_FAILED)
    db.add(gene)
    if pmcid_added:
        db.add(CrossReferenceModel(curie=f"PMCID:PMC{number}", curie_prefix="PMCID",
                                   reference_id=reference.reference_id))
    db.commit()
    db.execute(text("UPDATE workflow_tag SET date_created = :t, date_updated = :t WHERE reference_id = :r"),
               {"t": FAILED_AT, "r": reference.reference_id})
    if pmcid_added:
        db.execute(text("UPDATE cross_reference SET date_created = :t WHERE reference_id = :r"),
                   {"t": pmcid_added, "r": reference.reference_id})
    db.commit()
    return reference, gene.reference_workflow_tag_id


def _tags(db, reference):  # noqa
    db.expire_all()
    return sorted(w.workflow_tag_id for w in
                  db.query(WorkflowTagModel).filter(WorkflowTagModel.reference_id == reference.reference_id))


@pytest.fixture
def fb_retry_transition(db):  # noqa
    populate_test_mods()
    for mod in db.query(ModModel).filter(ModModel.abbreviation.in_(["FB", "WB"])):
        db.add(WorkflowTransitionModel(mod_id=mod.mod_id, transition_from=GENE_FAILED, transition_to=GENE_NEEDED,
                                       actions=["sub_task_retry::entity extraction"], condition="on_retry"))
    db.commit()


@patch("agr_literature_service.api.crud.ateam_db_helpers.load_name_to_atp_and_relationships",
       load_name_to_atp_and_relationships_mock)
@patch("agr_literature_service.lit_processing.data_check.check_wft_in_progress.send_report_to_slack")
class TestRetryWhenPmcidAdded:

    def test_failed_gene_extraction_is_retried_once_a_pmcid_is_added(self, _slack, db, fb_retry_transition):  # noqa
        load_name_to_atp_and_relationships_mock()
        late, _ = _paper(db, 1, pmcid_added=datetime(2026, 9, 20))
        check_wft_in_progress(db, debug=False)
        # months after the 6 week window: retried, and the main task is in progress again
        assert _tags(db, late) == [MAIN_IN_PROGRESS, GENE_NEEDED]

    def test_not_retried_without_a_new_pmcid(self, _slack, db, fb_retry_transition):  # noqa
        load_name_to_atp_and_relationships_mock()
        no_pmcid, _ = _paper(db, 2)
        pmcid_before, _ = _paper(db, 3, pmcid_added=datetime(2026, 6, 1))
        check_wft_in_progress(db, debug=False)
        # it would only fail again
        assert _tags(db, no_pmcid) == [MAIN_FAILED, GENE_FAILED]
        assert _tags(db, pmcid_before) == [MAIN_FAILED, GENE_FAILED]

    def test_only_for_fb(self, _slack, db, fb_retry_transition):  # noqa
        load_name_to_atp_and_relationships_mock()
        wb, _ = _paper(db, 4, mod_abbreviation="WB", pmcid_added=datetime(2026, 9, 20))
        check_wft_in_progress(db, debug=False)
        assert _tags(db, wb) == [MAIN_FAILED, GENE_FAILED]

    def test_debug_changes_nothing(self, _slack, db, fb_retry_transition):  # noqa
        load_name_to_atp_and_relationships_mock()
        late, _ = _paper(db, 5, pmcid_added=datetime(2026, 9, 20))
        check_wft_in_progress(db, debug=True)
        assert _tags(db, late) == [MAIN_FAILED, GENE_FAILED]
