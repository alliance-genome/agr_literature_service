"""Resource-descriptor URL resolution for person and laboratory cross references.

The reference-side cross_reference endpoints have always resolved a curie into a
`url` (and page names into {name, url}) from the A-team resource descriptors.
Person and laboratory cross references stored the same page names but never
resolved them, so the UI had no link to render. These tests cover the shared
resolver and the schema mixin that applies it.
"""
import pytest

from types import SimpleNamespace

import agr_literature_service.api.resource_descriptor_cache as rdc
from agr_literature_service.api.crud.cross_reference_crud import format_cross_reference_data
from agr_literature_service.api.resource_descriptor_cache import resolve_xref_urls
from agr_literature_service.api.schemas.laboratory_cross_reference_schemas import (
    LaboratoryCrossReferenceSchemaRelated,
    LaboratoryCrossReferenceSchemaShow,
)
# Importing laboratory_person_crud completes the parent read schemas: it is the
# module that resolves their TYPE_CHECKING forward refs and calls model_rebuild()
# (see the "Forward-reference resolution" block at the end of that file). The
# integration tests get this for free by importing the app.
import agr_literature_service.api.crud.laboratory_person_crud  # noqa: F401
from agr_literature_service.api.schemas.laboratory_schemas import LaboratorySchemaShow
from agr_literature_service.api.schemas.person_cross_reference_schemas import (
    PersonCrossReferenceSchemaRelated,
    PersonCrossReferenceSchemaShow,
)
from agr_literature_service.api.schemas.person_schemas import PersonSchemaShow


def seed_wb_and_orcid():
    rdc._seed([
        rdc.ResourceDescriptor(
            db_prefix="WB",
            name="WormBase",
            default_url="https://wormbase.org/species/all/person/[%s]",
            pages=[
                rdc.DescriptorPage(name="person", url="https://wormbase.org/resources/person/[%s]"),
                rdc.DescriptorPage(name="gene", url="https://wormbase.org/species/c_elegans/gene/[%s]"),
            ],
        ),
        rdc.ResourceDescriptor(
            db_prefix="ORCID",
            name="ORCID",
            default_url="https://orcid.org/[%s]",
        ),
    ])


class TestResolveXrefUrls:
    def test_default_url_substitutes_local_id(self):
        seed_wb_and_orcid()
        url, pages = resolve_xref_urls("ORCID:0000-0002-4689-7314", None)
        assert url == "https://orcid.org/0000-0002-4689-7314"
        assert pages is None

    def test_local_id_keeps_inner_colons(self):
        # Only the first colon separates prefix from local id.
        rdc._seed([rdc.ResourceDescriptor(db_prefix="DOI", default_url="https://doi.org/[%s]")])
        url, _ = resolve_xref_urls("DOI:10.1234/abc:def", None)
        assert url == "https://doi.org/10.1234/abc:def"

    def test_page_names_resolve_to_name_url_pairs(self):
        seed_wb_and_orcid()
        url, pages = resolve_xref_urls("WB:WBPerson1", ["person"])
        assert url == "https://wormbase.org/species/all/person/WBPerson1"
        assert pages == [{"name": "person", "url": "https://wormbase.org/resources/person/WBPerson1"}]

    def test_page_name_absent_from_descriptor_yields_no_url(self):
        seed_wb_and_orcid()
        _, pages = resolve_xref_urls("WB:WBPerson1", ["nonesuch"])
        assert pages == [{"name": "nonesuch", "url": None}]

    def test_unknown_prefix_yields_no_url_but_keeps_page_names(self):
        seed_wb_and_orcid()
        url, pages = resolve_xref_urls("NOPE:123", ["person"])
        assert url is None
        assert pages == [{"name": "person", "url": None}]

    def test_no_descriptors_at_all_is_not_an_error(self):
        rdc._seed([])
        assert resolve_xref_urls("WB:WBPerson1", None) == (None, None)

    def test_descriptor_without_default_url_yields_no_url(self):
        rdc._seed([rdc.ResourceDescriptor(db_prefix="WB", default_url=None)])
        url, _ = resolve_xref_urls("WB:WBPerson1", None)
        assert url is None

    @pytest.mark.parametrize("bad", ["nocolon", "", ":leading", "trailing:"])
    def test_malformed_curie_is_not_an_error(self, bad):
        seed_wb_and_orcid()
        url, pages = resolve_xref_urls(bad, ["person"])
        assert url is None
        assert pages == [{"name": "person", "url": None}]

    def test_empty_page_list_stays_empty(self):
        seed_wb_and_orcid()
        _, pages = resolve_xref_urls("WB:WBPerson1", [])
        assert pages == []


PERSON_SCHEMAS = [PersonCrossReferenceSchemaRelated, PersonCrossReferenceSchemaShow]
LAB_SCHEMAS = [LaboratoryCrossReferenceSchemaRelated, LaboratoryCrossReferenceSchemaShow]


def person_payload(**over):
    base = {
        "person_cross_reference_id": 1,
        "person_curie": "AGRKB:103000000000034",
        "curie": "WB:WBPerson1",
        "curie_prefix": "WB",
        "pages": ["person"],
        "is_obsolete": False,
    }
    base.update(over)
    return base


def lab_payload(**over):
    base = {
        "laboratory_cross_reference_id": 1,
        "laboratory_curie": "AGRKB:1030000000001",
        "curie": "WB:WBPerson1",
        "curie_prefix": "WB",
        "pages": ["person"],
        "is_obsolete": False,
    }
    base.update(over)
    return base


class TestSchemasResolveUrls:
    @pytest.mark.parametrize("schema", PERSON_SCHEMAS)
    def test_person_schema_gains_url_and_page_objects(self, schema):
        seed_wb_and_orcid()
        out = schema.model_validate(person_payload()).model_dump()
        assert out["url"] == "https://wormbase.org/species/all/person/WBPerson1"
        assert out["pages"] == [
            {"name": "person", "url": "https://wormbase.org/resources/person/WBPerson1"}
        ]

    @pytest.mark.parametrize("schema", LAB_SCHEMAS)
    def test_lab_schema_gains_url_and_page_objects(self, schema):
        seed_wb_and_orcid()
        out = schema.model_validate(lab_payload()).model_dump()
        assert out["url"] == "https://wormbase.org/species/all/person/WBPerson1"
        assert out["pages"] == [
            {"name": "person", "url": "https://wormbase.org/resources/person/WBPerson1"}
        ]

    @pytest.mark.parametrize("schema", PERSON_SCHEMAS + LAB_SCHEMAS)
    def test_null_pages_still_gets_a_default_url(self, schema):
        """The live person records have pages=NULL; they must still get a url."""
        seed_wb_and_orcid()
        payload = person_payload if schema in PERSON_SCHEMAS else lab_payload
        out = schema.model_validate(payload(pages=None)).model_dump()
        assert out["url"] == "https://wormbase.org/species/all/person/WBPerson1"
        assert out["pages"] is None

    @pytest.mark.parametrize("schema", PERSON_SCHEMAS + LAB_SCHEMAS)
    def test_unknown_prefix_leaves_url_none(self, schema):
        seed_wb_and_orcid()
        payload = person_payload if schema in PERSON_SCHEMAS else lab_payload
        out = schema.model_validate(
            payload(curie="NOPE:123", curie_prefix="NOPE", pages=None)
        ).model_dump()
        assert out["url"] is None

    @pytest.mark.parametrize("schema", PERSON_SCHEMAS + LAB_SCHEMAS)
    def test_already_resolved_pages_pass_through(self, schema):
        """Re-validating an already-serialized record must not double-wrap pages."""
        seed_wb_and_orcid()
        payload = person_payload if schema in PERSON_SCHEMAS else lab_payload
        out = schema.model_validate(
            payload(pages=[{"name": "person", "url": "https://example.org/x"}])
        ).model_dump()
        assert out["pages"] == [{"name": "person", "url": "https://example.org/x"}]


class TestNestedInParentRecord:
    """The UI reads person xrefs from GET /person/{curie}, i.e. nested inside
    PersonSchemaShow -- never from the cross-reference endpoints. Resolution has
    to reach that path or the Display page still has nothing to link to.
    """

    def test_person_record_nests_resolved_xrefs(self):
        seed_wb_and_orcid()
        person = PersonSchemaShow.model_validate({
            "person_id": 34,
            "display_name": "Cecilia Nakamura",
            "curie": "AGRKB:103000000000034",
            "cross_references": [
                person_payload(pages=None),
                person_payload(
                    person_cross_reference_id=2,
                    curie="ORCID:0000-0002-4689-7314",
                    curie_prefix="ORCID",
                    pages=None,
                ),
            ],
        }).model_dump()

        by_prefix = {x["curie_prefix"]: x for x in person["cross_references"]}
        assert by_prefix["WB"]["url"] == "https://wormbase.org/species/all/person/WBPerson1"
        assert by_prefix["ORCID"]["url"] == "https://orcid.org/0000-0002-4689-7314"

    def test_laboratory_record_nests_resolved_xrefs(self):
        seed_wb_and_orcid()
        lab = LaboratorySchemaShow.model_validate({
            "laboratory_id": 1,
            "curie": "AGRKB:1030000000001",
            "cross_references": [lab_payload(pages=["person"])],
        }).model_dump()

        xref = lab["cross_references"][0]
        assert xref["url"] == "https://wormbase.org/species/all/person/WBPerson1"
        assert xref["pages"] == [
            {"name": "person", "url": "https://wormbase.org/resources/person/WBPerson1"}
        ]


# --- the reference side now shares the same resolver -----------------------------
# format_cross_reference_data used to carry its own copy of the [%s] substitution.
# These pin its behaviour across that delegation; `db` is an unused parameter, so
# the function can be exercised directly without a session.

def xref_obj(curie):
    return SimpleNamespace(curie=curie,
                           resource=SimpleNamespace(curie="AGRKB:res1"),
                           reference=SimpleNamespace(curie="AGRKB:ref1"))


def data(curie, pages=None, resource_id=None, reference_id=None):
    return {"curie": curie, "pages": pages,
            "resource_id": resource_id, "reference_id": reference_id}


WB_RD = rdc.ResourceDescriptor(
    db_prefix="WB", name="WormBase",
    default_url="https://wormbase.org/default/[%s]",
    pages=[rdc.DescriptorPage(name="person", url="https://wormbase.org/person/[%s]")])


def test_default_url_and_pages():
    out = format_cross_reference_data(None, xref_obj("WB:WBPerson1"),
                                      data("WB:WBPerson1", ["person"]), {"WB": WB_RD})
    assert out["url"] == "https://wormbase.org/default/WBPerson1"
    assert out["pages"] == [{"name": "person", "url": "https://wormbase.org/person/WBPerson1"}]


def test_page_not_in_descriptor_gets_empty_url():
    out = format_cross_reference_data(None, xref_obj("WB:WBPerson1"),
                                      data("WB:WBPerson1", ["nope"]), {"WB": WB_RD})
    assert out["pages"] == [{"name": "nope", "url": None}]


def test_no_descriptor_keeps_page_names_without_url():
    out = format_cross_reference_data(None, xref_obj("XX:1"), data("XX:1", ["person"]), {})
    assert out["pages"] == [{"name": "person", "url": None}]
    assert "url" not in out


def test_no_descriptor_no_pages():
    out = format_cross_reference_data(None, xref_obj("XX:1"), data("XX:1", None), {})
    assert out["pages"] is None
    assert "url" not in out


def test_curie_without_colon_still_normalises_pages():
    """Previously returned early leaving raw strings in `pages`, which would
    then fail CrossReferenceSchemaShow validation."""
    out = format_cross_reference_data(None, xref_obj("nocolon"), data("nocolon", ["person"]), {"WB": WB_RD})
    assert out["pages"] == [{"name": "person", "url": None}]
    assert "url" not in out


def test_resource_and_reference_curies_replace_ids():
    out = format_cross_reference_data(None, xref_obj("WB:WBPerson1"),
                                      data("WB:WBPerson1", None, resource_id=5, reference_id=7),
                                      {"WB": WB_RD})
    assert out["resource_curie"] == "AGRKB:res1"
    assert out["reference_curie"] == "AGRKB:ref1"
    assert "resource_id" not in out and "reference_id" not in out


def test_schema_output_unchanged_by_shape_convergence():
    """`url: ""` vs `url: None`, and a missing url key, all normalise to the same
    JSON through CrossReferenceSchemaShow -- so converging the dict shape is not
    an API change."""
    from agr_literature_service.api.schemas.cross_reference_schemas import (
        CrossReferencePageSchemaShow,
    )
    assert (CrossReferencePageSchemaShow.model_validate({"name": "p", "url": ""}).model_dump()
            == {"name": "p", "url": ""})
    assert (CrossReferencePageSchemaShow.model_validate({"name": "p"}).model_dump()
            == {"name": "p", "url": None})
    assert (CrossReferencePageSchemaShow.model_validate({"name": "p", "url": None}).model_dump()
            == {"name": "p", "url": None})
