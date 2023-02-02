import logging
from agr_literature_service.api.models import ReferenceModel, CrossReferenceModel, \
    ReferencefileModel, AuthorModel, MeshDetailModel
from agr_literature_service.lit_processing.utils.db_read_utils import \
    get_references_by_curies, get_curie_to_title_mapping, \
    get_pmid_list_without_pmc_package, get_pmid_to_reference_id_mapping, \
    get_reference_id_by_curie, retrieve_newly_added_pmids, retrieve_all_pmids, \
    get_reference_id_by_pmid, get_cross_reference_data, get_reference_ids_by_pmids, \
    get_doi_data, get_author_data, get_mesh_term_data, get_mod_abbreviations
from agr_literature_service.lit_processing.data_ingest.utils.db_write_utils import \
    insert_referencefile, insert_referencefile_mod_for_pmc
from ...fixtures import db, load_sanitized_references, populate_test_mod_reference_types # noqa

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class TestDbReadUtils:

    def test_dqm_db_read_functions(self, db, load_sanitized_references, populate_test_mod_reference_types): # noqa

        refs = db.query(ReferenceModel).order_by(ReferenceModel.curie).all()

        db_entries = get_references_by_curies(db, [refs[0].curie, refs[1].curie])
        assert db_entries[refs[0].curie]['issue_name'] == '1'
        assert db_entries[refs[0].curie]['volume'] == '888'
        assert db_entries[refs[0].curie]['page_range'] == '88'
        assert db_entries[refs[1].curie]['issue_name'] == '66'
        assert db_entries[refs[1].curie]['volume'] == '4'
        assert db_entries[refs[1].curie]['page_range'] == '937'

        missing_agr_in_mod = {}
        missing_agr_in_mod['ZFIN'] = [refs[0].curie, refs[1].curie]
        agr_to_title = get_curie_to_title_mapping(missing_agr_in_mod['ZFIN'])
        assert agr_to_title[refs[0].curie] == refs[0].title
        assert agr_to_title[refs[1].curie] == refs[1].title

        reference_id = get_reference_id_by_curie(db, refs[0].curie)
        assert reference_id == refs[0].reference_id

        pmids = retrieve_newly_added_pmids(db)
        pmids_db = []
        test_pmid = None
        test_reference_id = None
        for x in db.query(CrossReferenceModel).filter_by(curie_prefix='PMID').all():
            if test_pmid is None:
                test_pmid = x.curie.replace("PMID:", '')
                test_reference_id = x.reference_id
            pmids_db.append(x.curie.replace("PMID:", ''))
        pmids.sort()
        pmids_db.sort()
        assert pmids == pmids_db

        all_pmids = retrieve_all_pmids(db)
        all_pmids.sort()
        assert all_pmids == pmids_db

        reference_id = get_reference_id_by_pmid(db, test_pmid)
        assert reference_id == test_reference_id

        (reference_id_to_doi, reference_id_to_pmcid) = get_cross_reference_data(db, None, [refs[0].reference_id, refs[1].reference_id])
        for x in db.query(CrossReferenceModel).filter_by(reference_id=refs[0].reference_id).all():
            if x.curie_prefix == 'DOI':
                assert x.curie.replace("DOI:", "") == reference_id_to_doi[refs[0].reference_id]
            if x.curie_prefix == 'PMCID':
                assert x.curie.replace("PMCID:", "") == reference_id_to_pmcid[refs[0].reference_id]

        pmid_to_reference_id = {}
        reference_id_to_pmid = {}
        get_reference_ids_by_pmids(db, "|".join(pmids), pmid_to_reference_id, reference_id_to_pmid)

        doi_to_reference_id = get_doi_data(db)

        for x in db.query(CrossReferenceModel).all():
            if x.curie.replace == 'PMID':
                pmid = x.curie.replace("PMID:", "")
                assert pmid_to_reference_id[pmid] == x.reference_id
                assert reference_id_to_pmid[x.reference_id] == pmid
            if x.curie.replace == 'DOI':
                assert doi_to_reference_id[x.curie] == x.reference_id

        reference_id_to_authors = get_author_data(db, 'ZFIN',
                                                  [refs[0].reference_id, refs[1].reference_id], 500)
        authors = reference_id_to_authors[refs[0].reference_id]

        x = db.query(AuthorModel).filter_by(reference_id=refs[0].reference_id, order=1).one_or_none()
        assert x.last_name == authors[0]['last_name']
        assert x.name == authors[0]['name']

        reference_id_to_mesh_terms = get_mesh_term_data(db, 'ZFIN',
                                                        [refs[0].reference_id, refs[1].reference_id],
                                                        500)
        mesh_terms = reference_id_to_mesh_terms[refs[0].reference_id]
        (heading_term, qualifier_term) = mesh_terms[0]
        mesh_terms = db.query(MeshDetailModel).filter_by(reference_id=refs[0].reference_id).all()
        assert mesh_terms[0].heading_term == heading_term
        assert mesh_terms[0].qualifier_term == qualifier_term

        mods = get_mod_abbreviations(db)
        mods.sort()
        assert mods == ['FB', 'MGI', 'RGD', 'SGD', 'WB', 'XB', 'ZFIN']


    def test_pmc_read_and_write_functions(self, db, load_sanitized_references, populate_test_mod_reference_types): # noqa

        pmid = "33622238"
        crossRef = db.query(CrossReferenceModel).filter_by(curie='PMID:' + pmid).one_or_none()
        reference_id = crossRef.reference_id
        file_class = 'supplement'
        file_publication_status = 'final'
        file_name_with_suffix = "test_suppl.txt"
        md5sum = "d5073c77841aa7ae1066dbd2323dcd56"
        referencefile_id = insert_referencefile(db, pmid, file_class,
                                                file_publication_status,
                                                file_name_with_suffix,
                                                reference_id, md5sum,
                                                logger)
        insert_referencefile_mod_for_pmc(db, pmid, file_name_with_suffix,
                                         referencefile_id, logger)
        db.commit()

        refFile = db.query(ReferencefileModel).filter_by(reference_id=reference_id).one_or_none()
        assert refFile.display_name == 'test_suppl'
        assert refFile.file_extension == 'txt'
        assert refFile.md5sum == md5sum

        pmids = get_pmid_list_without_pmc_package(['ZFIN'], db)
        assert pmids == ['34354223', '35151207']

        pmid_to_reference_id = get_pmid_to_reference_id_mapping(db)
        assert pmid_to_reference_id.get(pmid) == reference_id
