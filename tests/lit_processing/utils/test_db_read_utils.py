import logging
from agr_literature_service.api.models import ReferenceModel
from agr_literature_service.lit_processing.utils.db_read_utils import \
    get_references_by_curies, get_curie_to_title_mapping
from ...fixtures import db, load_sanitized_references, populate_test_mod_reference_types # noqa

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class TestDbReadUtils:

    def test_dqm_read_functions(self, db, load_sanitized_references, populate_test_mod_reference_types): # noqa

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
