from os import path

from agr_literature_service.api.models import ResourceModel, CrossReferenceModel
from agr_literature_service.lit_processing.data_ingest.post_resource_to_db import \
    post_resources
from agr_literature_service.lit_processing.tests.mod_populate_load import populate_test_mods
from agr_literature_service.lit_processing.utils.tmp_files_utils import init_tmp_dir
from ...fixtures import db, cleanup_tmp_files_when_done # noqa


class TestPostResourceToDb:

    def test_post_resources(self, db): # noqa
        init_tmp_dir()
        populate_test_mods()

        base_input_dir = path.join(path.dirname(__file__), "../sample_data/")
        json_path = "sanitized_resources/"
        post_resources(db, json_path, 'NLM', base_input_dir)

        res_rows = db.query(ResourceModel).order_by(ResourceModel.resource_id).all()
        assert len(res_rows) == 5
        assert res_rows[0].title == 'Biochemical and biophysical research communications'
        assert res_rows[1].iso_abbreviation == 'J Physiol Sci'
        assert res_rows[2].online_issn == '2399-3642'
        assert res_rows[3].print_issn == '1552-5260'

        resource_id = res_rows[4].resource_id
        crossRef = db.query(CrossReferenceModel).filter_by(resource_id=resource_id).first()
        assert crossRef.curie == 'NLM:101759238'
