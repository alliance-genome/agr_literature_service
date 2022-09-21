import os
from os import environ

import pytest

from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.get_pubmed_xml import download_pubmed_xml
from ....fixtures import cleanup_tmp_files_when_done # noqa


class TestGetPubmedXML:

    @pytest.mark.webtest
    def test_download_pubmed_xml(self, cleanup_tmp_files_when_done): # noqa
        base_path = environ.get('XML_PATH')
        download_pubmed_xml(["88888"])
        assert os.path.exists(os.path.join(base_path, "pubmed_xml", "88888.xml"))
