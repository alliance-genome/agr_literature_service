"""person / laboratory xref curie-pattern tests.

Pattern matching + yml loading are exercised directly through ``patterns_check``
(no db, no auth, run locally). The new ``/check/...`` routes are asserted present
via ``app.routes`` (auth-free). Full authed endpoint behavior runs in CI (needs
Cognito), like the reference/resource check tests in ``test_cross_ref.py``.
"""
from agr_literature_service.api.crud.utils import patterns_check
from agr_literature_service.api.main import app

VALID = {
    "person": [
        "ZFIN:ZDB-PERS-030131-1", "Xenbase:XB-PERS-123",
        "WB:WBPerson123", "SGD:Colleague_1269583",
    ],
    "laboratory": [
        "ZFIN:ZDB-LAB-030131-1", "Xenbase:XB-LAB-123",
        "WB:PS", "SGD:Lab_1273663",
    ],
}


def test_patterns_maps_have_all_four_prefixes():
    p = patterns_check.get_patterns()
    for datatype in ("person", "laboratory"):
        assert set(p[datatype]) == {"ZFIN", "Xenbase", "WB", "SGD"}


def test_valid_curies_match():
    for datatype, curies in VALID.items():
        for curie in curies:
            assert patterns_check.check_pattern(datatype, curie) is True, (datatype, curie)


def test_trailing_junk_rejected_by_anchor():
    # valid prefix + body + trailing junk must fail -> proves the $ anchor
    assert patterns_check.check_pattern("person", "SGD:Colleague_1269583X") is False
    assert patterns_check.check_pattern("laboratory", "WB:PSTU") is False  # 4 caps > {2,3}


def test_unknown_prefix_returns_none():
    assert patterns_check.check_pattern("person", "MGI:12345") is None
    assert patterns_check.check_pattern("laboratory", "FB:FBrf0111489") is None


def test_person_and_lab_bodies_are_distinct():
    # SGD (and the others) exist in both datatypes, but the person "Colleague_" body
    # must not satisfy the laboratory "Lab_" pattern, and vice versa.
    assert patterns_check.check_pattern("laboratory", "SGD:Colleague_1269583") is False
    assert patterns_check.check_pattern("person", "SGD:Lab_1273663") is False


def test_check_routes_present_on_entity_routers():
    paths = {r.path for r in app.routes}
    for base in ("/person_cross_reference", "/laboratory_cross_reference"):
        assert base + "/check/patterns" in paths
        assert any(p.startswith(base + "/check/curie/") for p in paths)
