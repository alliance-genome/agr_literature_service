"""report_duplicate_orcid_in_reference runs its SQL against the real schema (SCRUM-6936).

The monthly job failed on 2026-11-18 with "column r.reference_id does not exist": its raw SQL
still used the primary-key names that SCRUM-6719 renamed to id. Running the query against the
test database makes a future rename fail CI instead of the next monthly run.
"""
from agr_literature_service.api.models import AuthorModel, ModCorpusAssociationModel, ModModel, ReferenceModel
from agr_literature_service.api.schemas import ModCorpusSortSourceType
from agr_literature_service.lit_processing.data_check.report_duplicate_orcid_in_reference import (
    get_all_author_duplicate_orcid_reference_for_mod
)
from agr_literature_service.lit_processing.tests.mod_populate_load import populate_test_mods
from ...fixtures import db  # noqa

ORCID = "ORCID:0000-0002-1825-0097"


def _paper(db, number, in_corpus, author_orcids):  # noqa
    """A WB paper whose authors (in order) have the given ORCIDs."""
    wb = db.query(ModModel).filter(ModModel.abbreviation == "WB").one()
    reference = ReferenceModel(curie=f"AGRKB:10100060{number:04d}", category="research_article")
    db.add(reference)
    db.flush()
    db.add(ModCorpusAssociationModel(reference_id=reference.reference_id, mod_id=wb.mod_id, corpus=in_corpus,
                                     mod_corpus_sort_source=ModCorpusSortSourceType.Dqm_files))
    for order, orcid in enumerate(author_orcids, start=1):
        db.add(AuthorModel(reference_id=reference.reference_id, author_order=order,
                           name=f"Author {order}", orcid=orcid))
    db.commit()
    return reference.curie


def test_reports_in_corpus_papers_whose_authors_share_an_orcid(db):  # noqa
    populate_test_mods()
    duplicate = _paper(db, 1, True, [ORCID, None, ORCID])
    _paper(db, 2, True, [ORCID, "ORCID:0000-0001-5109-3700"])  # different ORCIDs
    _paper(db, 3, False, [ORCID, ORCID])  # not in the corpus

    rows = get_all_author_duplicate_orcid_reference_for_mod(db, "WB")

    assert [tuple(row) for row in rows] == [(duplicate, ORCID, "Author 1 | Author 3")]
    assert get_all_author_duplicate_orcid_reference_for_mod(db, "FB") == []
