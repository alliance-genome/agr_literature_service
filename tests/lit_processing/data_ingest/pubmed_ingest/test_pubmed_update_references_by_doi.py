from agr_literature_service.api.models import CrossReferenceModel
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.pubmed_update_references_by_doi import \
    get_pmid_for_doi, add_pmid_to_existing_papers
from ....fixtures import db, load_sanitized_references, cleanup_tmp_files_when_done, \
    populate_test_mod_reference_types # noqa


class TestPubmedUpdateReferenceByDoi:

    def test_add_pmid_to_existing_papers(self, db, load_sanitized_references, cleanup_tmp_files_when_done): # noqa

        cr = db.query(CrossReferenceModel).filter_by(curie='FB:FBrf0066905').one_or_none()
        reference_id = cr.reference_id
        pmid_to_add = 'PMID:88888'
        papers_to_add_pmid = [(reference_id, pmid_to_add)]
        add_pmid_to_existing_papers(db, papers_to_add_pmid)
        cr = db.query(CrossReferenceModel).filter_by(reference_id=reference_id, curie_prefix='PMID').one_or_none()
        assert cr.curie == pmid_to_add

    def test_get_pmid_for_doi(self):
        doi = "DOI:10.1007/s00427-004-0409-1"
        pmids = get_pmid_for_doi(doi)
        assert pmids[0] == '15221377'
