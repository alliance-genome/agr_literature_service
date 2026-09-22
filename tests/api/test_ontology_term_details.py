"""Unit tests for get_topic_term_details (POST /ontology/term_details,
SCRUM-6168). The A-team client is monkeypatched, so no network or database is
needed. Covers: the empty-list short-circuit, skipping curies whose term is
None (no matching non-obsolete term), the response mapping shape, and the
request-size cap.
"""
import json
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from agr_literature_service.api.crud import ateam_db_helpers
from agr_literature_service.api.crud.ateam_db_helpers import (
    MAX_TERM_DETAILS_CURIES,
    get_topic_term_details,
)


def _term(curie, name, definition=None, synonyms=None):
    return SimpleNamespace(curie=curie, name=name, definition=definition,
                           synonyms=synonyms)


def _fake_client(terms_by_curie):
    return SimpleNamespace(get_ontology_terms=lambda curies: terms_by_curie)


def _body(response):
    return json.loads(response.body)


def test_empty_list_short_circuits_without_client(monkeypatch):
    # No client is constructed at all for an empty request.
    def boom():
        raise AssertionError("client should not be created for an empty list")
    monkeypatch.setattr(ateam_db_helpers, "_get_client", boom)
    assert _body(get_topic_term_details([])) == {}


def test_maps_terms_and_skips_none(monkeypatch):
    terms = {
        "ATP:0000122": _term("ATP:0000122", "RNAi phenotype",
                             definition="A phenotype from RNAi.",
                             synonyms=["RNAi"]),
        "ATP:0000999": None,  # no matching non-obsolete term -> omitted
        "ATP:0000005": _term("ATP:0000005", "gene"),  # None definition/synonyms
    }
    monkeypatch.setattr(ateam_db_helpers, "_get_client",
                        lambda: _fake_client(terms))
    data = _body(get_topic_term_details(list(terms.keys())))
    assert set(data.keys()) == {"ATP:0000122", "ATP:0000005"}
    assert data["ATP:0000122"] == {
        "curie": "ATP:0000122",
        "name": "RNAi phenotype",
        "definition": "A phenotype from RNAi.",
        "synonyms": ["RNAi"],
    }
    # None synonyms normalize to []; None definition passes through.
    assert data["ATP:0000005"]["synonyms"] == []
    assert data["ATP:0000005"]["definition"] is None


def test_none_result_from_client_is_empty_mapping(monkeypatch):
    monkeypatch.setattr(ateam_db_helpers, "_get_client",
                        lambda: _fake_client(None))
    assert _body(get_topic_term_details(["ATP:0000122"])) == {}


def test_request_size_cap(monkeypatch):
    def boom():
        raise AssertionError("client should not be created past the cap")
    monkeypatch.setattr(ateam_db_helpers, "_get_client", boom)
    too_many = [f"ATP:{i:07d}" for i in range(MAX_TERM_DETAILS_CURIES + 1)]
    with pytest.raises(HTTPException) as exc:
        get_topic_term_details(too_many)
    assert exc.value.status_code == 422
