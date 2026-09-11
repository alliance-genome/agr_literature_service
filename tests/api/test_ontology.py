import pytest

from agr_literature_service.api.crud.topic_entity_tag_utils import get_ancestors, get_descendants
from tests.fixtures import load_name_to_atp_and_relationships_mock
from .fixtures import auth_headers # noqa


class TestOntology:

    @pytest.mark.webtest
    def test_get_ancestors(self, auth_headers):  # noqa
        load_name_to_atp_and_relationships_mock()
        onto_node = "ATP:0000079"
        ancestors = get_ancestors(onto_node)
        expected_ancestors = {"ATP:0000001", "ATP:0000002", "ATP:0000009"}
        assert [ancestor in expected_ancestors for ancestor in ancestors]

    @pytest.mark.webtest
    def test_get_descendants(self, auth_headers):  # noqa
        load_name_to_atp_and_relationships_mock()
        onto_node = "ATP:0000009"
        descendants = get_descendants(onto_node)
        expected_descendants = {'ATP:0000079', 'ATP:0000080', 'ATP:0000081', 'ATP:0000082', 'ATP:0000083',
                                'ATP:0000084', 'ATP:0000085', 'ATP:0000086', 'ATP:0000087', 'ATP:0000033',
                                'ATP:0000034', 'ATP:0000100'}
        assert [ancestor in expected_descendants for ancestor in descendants]

    @pytest.mark.webtest
    def test_get_ancestors_non_existent(self, auth_headers):  # noqa
        load_name_to_atp_and_relationships_mock()
        onto_node = "ATP:000007"
        ancestors = get_ancestors(onto_node)
        assert len(ancestors) == 0


class TestAtpBfsStartTerms:
    """SCRUM-5746: the ATP cache must cover every root the validation rules traverse."""

    # Three top-level branches plus the data_context subtree, as the ontology returns them.
    FAKE_ONTOLOGY = {
        "ATP:0000177": [("ATP:0000172", "workflow subprocess")],
        "ATP:0000335": [("ATP:0000334", "existing data"), ("ATP:0000321", "novel data")],
        "ATP:0000323": [("ATP:0000324", "mentioned data"), ("ATP:0000326", "marker data")],
        "ATP:0000324": [("ATP:0000360", "background information"),
                        ("ATP:0000325", "experimentally studied data")],
        "ATP:0000326": [("ATP:0000328", "expression marker"),
                        ("ATP:0000327", "genetic marker")],
    }

    def test_default_start_terms_resolve_data_context_ancestors(self, monkeypatch):
        """validate_tags climbs ancestors of data_novelty AND data_context.

        ATP:0000323 (data context) is a separate top-level branch from ATP:0000335 (data
        novelty), so unless it is one of the BFS start terms the traversal never reaches
        it. atp_get_all_ancestors only falls back to the ontology client when
        atp_to_parent is *entirely* empty, so in any warm worker a missing branch would
        resolve to no ancestors and the data_context validation check would silently
        degrade to exact equality.

        The client is stubbed rather than _get_atp_children, so the real loader and the
        real caching path run -- the bug this guards against lived in which terms the
        traversal visits, not in what the cache does with them. Deliberately not using
        load_name_to_atp_and_relationships_mock: that injects the whole hierarchy through
        set_globals, which is precisely why the suite could not see the original bug.
        """
        from agr_literature_service.api.crud import ateam_db_helpers as helpers

        class FakeClient:
            def get_atp_descendants(self, ancestor_curie, direct_children_only=False):
                return [{"curie": c, "name": n}
                        for c, n in TestAtpBfsStartTerms.FAKE_ONTOLOGY.get(ancestor_curie, [])]

        # The loader clears all four module globals, and nothing resets them between
        # tests, so snapshot and restore them or later tests inherit a cold cache.
        saved = (dict(helpers.atp_to_name), dict(helpers.name_to_atp),
                 dict(helpers.atp_to_children), dict(helpers.atp_to_parent))
        monkeypatch.setattr(helpers, "_get_client", lambda: FakeClient())
        monkeypatch.setattr(helpers, "_fetch_atp_names", lambda curies: None)
        try:
            helpers.load_name_to_atp_and_relationships()   # default start terms
            assert helpers.atp_get_all_ancestors("ATP:0000334") == ["ATP:0000335"], \
                "data_novelty must keep resolving"
            assert helpers.atp_get_all_ancestors("ATP:0000325") == [
                "ATP:0000324", "ATP:0000323"], \
                "data_context ancestors must resolve from the default start terms"
        finally:
            helpers.set_globals(*saved)
