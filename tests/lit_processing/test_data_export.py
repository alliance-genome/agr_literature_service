import pytest

from agr_literature_service.lit_processing.tests.mod_populate_load import post_mods
from ..fixtures import db # noqa


@pytest.fixture
def test_data(db):
    print("***** Loading data into the DB *****")
    print("***** Adding mods *****")
    post_mods()
    print("***** Adding references *****")
    yield None
    print("***** Cleaning up data *****")


class TestDataExport:

    def test_dump_data(self, test_data): # noqa
        assert True
