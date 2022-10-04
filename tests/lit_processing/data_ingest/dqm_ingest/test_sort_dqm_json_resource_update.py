import os
# import pytest
# from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.lit_processing.data_ingest.dqm_ingest.sort_dqm_json_resource_updates \
    import update_sanitized_resources
from agr_literature_service.lit_processing.data_ingest.post_resource_to_db \
    import post_resources
from agr_literature_service.api.models import ResourceModel
from ....fixtures import db # noqa

import json


class TestParseDqmJsonResource:
    # @pytest.fixture
    def test_zfin_resource_parse(self, db): # noqa
        # Test loading of ZFIN data, Update tests later.
        # base_path = environ.get('XML_PATH')
        sample_file_path = os.path.join(
            os.path.dirname(__file__),
            "../../sample_data/sanitized_resources/")
        datatype = 'ZFIN'
        filename = sample_file_path + 'RESOURCE_' + datatype + '.json'
        # This produces a json fiel fro new resources and writes to the db
        # changes of those that exist already.
       
        update_sanitized_resources(db, 'ZFIN', filename)

        # So check the json for the correct info.
        
        # now load the file
        # /usr/local/bin/src/literature/agr_literature_service/lit_processing/tests/tmp/sanitized_resource_json_updates/
        f = open("/usr/local/bin/src/literature/agr_literature_service/lit_processing/tests/tmp/sanitized_resource_json_updates/RESOURCE_ZFIN.json")
        resource_data = json.load(f)
        print(resource_data)

        post_resources(db, "sanitized_resource_json_updates", "ZFIN")
        json_storage_path = sample_file_path + '/'
        json_filename = json_storage_path + 'RESOURCE_' + datatype + '.json'
        print(f"fake path for now is {json_filename}")
        print("resources are:-")
        for bob in db.query(ResourceModel).all():
            print(bob)
            print(bob.title)
            print(f"\t{bob.print_issn}=>pi")
            if bob.title == "Advances in biology laboratory education : p1":
                print(f"{bob.title} FOUND!")
        res = db.query(ResourceModel).filter_by(title='Advances in biology laboratory education : p1').one_or_none()
        print(dir(res))
        print(res)
        assert res.print_issn == "2769-1810-NOOOOOOO"
