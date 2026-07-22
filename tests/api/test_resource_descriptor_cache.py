from unittest import mock

import agr_literature_service.api.resource_descriptor_cache as rdc


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


def test_normalize_maps_ateam_fields():
    rd = rdc._normalize_ateam_descriptor(ATEAM_SAMPLE[0])
    assert rd == rdc.ResourceDescriptor(
        db_prefix="TESTMOD",
        name="Test Mod",
        aliases=["TM", "TMOD"],
        default_url="http://test.org/[%s]",
        pages=[
            rdc.DescriptorPage(name="gene", url="http://test.org/gene/[%s]"),
            rdc.DescriptorPage(name="homepage", url="http://test.org/"),
        ],
    )


def test_normalize_skips_without_prefix():
    assert rdc._normalize_ateam_descriptor({"name": "no prefix"}) is None


def test_fetch_from_ateam_uses_client():
    with mock.patch("agr_curation_api.AGRCurationAPIClient") as MockClient:
        MockClient.return_value.get_resource_descriptors.return_value = ATEAM_SAMPLE
        result = rdc._fetch_from_ateam()
    assert [r.db_prefix for r in result] == ["TESTMOD"]
    assert result[0].pages[0].name == "gene"
