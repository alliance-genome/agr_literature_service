import os
from os import environ

import pytest

from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.get_pubmed_xml import download_pubmed_xml
from tests.utils import cleanup_tmp_files


class TestGetPubmedXML:

    @pytest.mark.webtest
    def test_download_pubmed_xml(self):
        try:
            base_path = environ.get('XML_PATH')
            download_pubmed_xml(["88888"])
            assert os.path.exists(os.path.join(base_path, "pubmed_xml", "88888.xml"))
        finally:
            cleanup_tmp_files()
