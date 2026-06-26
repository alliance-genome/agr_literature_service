from unittest import mock

from starlette.testclient import TestClient
from fastapi import status

import agr_literature_service.api.initialize as initialize
from agr_literature_service.api.main import app
from agr_literature_service.api.models.resource_descriptor_models import (
    ResourceDescriptorModel,
)
from ..fixtures import db  # noqa
from .fixtures import auth_headers  # noqa


# A sample A-Team `findForPublic` style descriptor (camelCase keys).
ATEAM_SAMPLE = [
    {
        "prefix": "TESTMOD",
        "name": "Test Mod",
        "synonyms": ["TM", "TMOD"],
        "idExample": "TESTMOD:123",
        "idPattern": r"^TESTMOD:\d+$",
        "defaultUrlTemplate": "http://test.org/[%s]",
        "resourcePages": [
            {"name": "gene", "urlTemplate": "http://test.org/gene/[%s]"},
            {"name": "homepage", "urlTemplate": "http://test.org/"},
        ],
    }
]

# A sample agr_schemas YAML style descriptor (canonical keys, with example_id).
YAML_SAMPLE = [
    {
        "db_prefix": "YAMLMOD",
        "name": "Yaml Mod",
        "example_id": "YAMLMOD:9",
        "default_url": "http://yaml.org/[%s]",
        "pages": [{"name": "homepage", "url": "http://yaml.org/"}],
    }
]


class TestResourceDescriptor:

    def test_get_res_des(self, auth_headers):  # noqa
        with TestClient(app) as client:
            response = client.get(url="/resource_descriptor", headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK

    def test_normalize_ateam_descriptor(self):
        normalized = initialize._normalize_ateam_descriptor(ATEAM_SAMPLE[0])
        assert normalized == {
            "db_prefix": "TESTMOD",
            "name": "Test Mod",
            "aliases": ["TM", "TMOD"],
            "example_gid": "TESTMOD:123",
            "gid_pattern": r"^TESTMOD:\d+$",
            "default_url": "http://test.org/[%s]",
            "pages": [
                {"name": "gene", "url": "http://test.org/gene/[%s]"},
                {"name": "homepage", "url": "http://test.org/"},
            ],
        }

    def test_normalize_ateam_descriptor_skips_without_prefix(self):
        assert initialize._normalize_ateam_descriptor({"name": "no prefix"}) is None

    def test_load_from_ateam(self, db):  # noqa
        with mock.patch("agr_curation_api.AGRCurationAPIClient") as MockClient:
            MockClient.return_value.get_resource_descriptors.return_value = ATEAM_SAMPLE
            initialize.update_resource_descriptor(db)

        rd = db.query(ResourceDescriptorModel).filter_by(db_prefix="TESTMOD").one()
        assert rd.name == "Test Mod"
        assert rd.aliases == ["TM", "TMOD"]
        assert rd.example_gid == "TESTMOD:123"
        assert rd.gid_pattern == r"^TESTMOD:\d+$"
        assert rd.default_url == "http://test.org/[%s]"
        pages = sorted(rd.pages, key=lambda p: p.name)
        assert [(p.name, p.url) for p in pages] == [
            ("gene", "http://test.org/gene/[%s]"),
            ("homepage", "http://test.org/"),
        ]

    def test_fallback_to_yaml_on_ateam_error(self, db):  # noqa
        with mock.patch.object(
            initialize, "_fetch_descriptors_from_ateam",
            side_effect=RuntimeError("boom")
        ), mock.patch.object(
            initialize, "_fetch_descriptors_from_yaml",
            return_value=YAML_SAMPLE
        ) as yaml_mock:
            initialize.update_resource_descriptor(db)
            yaml_mock.assert_called_once()

        rd = db.query(ResourceDescriptorModel).filter_by(db_prefix="YAMLMOD").one()
        # example_id should be mapped onto the example_gid column.
        assert rd.example_gid == "YAMLMOD:9"
        assert rd.default_url == "http://yaml.org/[%s]"
        assert [(p.name, p.url) for p in rd.pages] == [("homepage", "http://yaml.org/")]

    def test_fallback_to_yaml_when_ateam_empty(self, db):  # noqa
        with mock.patch.object(
            initialize, "_fetch_descriptors_from_ateam", return_value=[]
        ), mock.patch.object(
            initialize, "_fetch_descriptors_from_yaml", return_value=YAML_SAMPLE
        ) as yaml_mock:
            initialize.update_resource_descriptor(db)
            yaml_mock.assert_called_once()

        assert db.query(ResourceDescriptorModel).filter_by(db_prefix="YAMLMOD").one()
