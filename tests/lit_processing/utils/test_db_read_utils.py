import logging
from agr_literature_service.api.models import ReferenceModel, CrossReferenceModel, \
    ReferencefileModel
from agr_literature_service.lit_processing.utils.db_read_utils import \
    get_references_by_curies, get_curie_to_title_mapping, \
    get_pmid_list_without_pmc_package, get_pmid_to_reference_id_mapping
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
