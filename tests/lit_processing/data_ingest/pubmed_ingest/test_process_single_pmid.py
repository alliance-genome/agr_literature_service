import os
from os import environ

import pytest
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.process_single_pmid import process_pmid
from ....fixtures import cleanup_tmp_files_when_done # noqa


class TestProcessSinglePMID:

    @pytest.mark.webtest
    def test_process_pmid(self, cleanup_tmp_files_when_done): # noqa
        base_path = environ.get('XML_PATH')
        process_pmid("12345")
        assert os.path.exists(os.path.join(base_path, "pubmed_xml", "12345.xml"))
