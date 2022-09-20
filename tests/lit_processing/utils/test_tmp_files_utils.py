import os.path
from os import environ

from agr_literature_service.lit_processing.utils.tmp_files_utils import init_tmp_dir
from ...fixtures import cleanup_tmp_files_when_done # noqa


class TestTmpFilesUtils:
    def test_init_tmp_dir(self, cleanup_tmp_files_when_done):
        init_tmp_dir()
        assert os.path.exists(environ.get("XML_PATH", ""))
