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

    def test_default_start_terms_resolve_data_context_ancestors(self):
        """validate_tags climbs ancestors of data_novelty AND data_context.

        ATP:0000323 (data context) is a separate top-level branch from ATP:0000335 (data
        novelty), so unless it is one of the BFS start terms the traversal never reaches
        it and atp_to_parent holds no data_context pointers. atp_get_all_ancestors only
        falls back to the ontology client when that map is *entirely* empty, so in any
        warm worker it would return [] and the data_context validation check would
        silently degrade to exact equality.
        """
        from agr_literature_service.api.crud import ateam_db_helpers as helpers

        fake_children = {
            "ATP:0000177": ["ATP:0000172"], "ATP:0000172": [],
            "ATP:0000335": ["ATP:0000334", "ATP:0000321"],
            "ATP:0000334": [], "ATP:0000321": [],
            "ATP:0000323": ["ATP:0000324", "ATP:0000326"],
            "ATP:0000324": ["ATP:0000360", "ATP:0000325"],
            "ATP:0000326": ["ATP:0000328", "ATP:0000327"],
            "ATP:0000325": [], "ATP:0000360": [],
            "ATP:0000327": [], "ATP:0000328": [],
        }

        def fake_get_children(parent):
            for child in fake_children.get(parent, []):
                helpers.atp_to_parent[child] = parent
                helpers.atp_to_children.setdefault(parent, []).append(child)
            return fake_children.get(parent, [])

        real_children, real_names = helpers._get_atp_children, helpers._fetch_atp_names
        try:
            helpers._get_atp_children = fake_get_children
            helpers._fetch_atp_names = lambda curies: None
            helpers.load_name_to_atp_and_relationships()   # default start terms
            assert helpers.atp_get_all_ancestors("ATP:0000334") == ["ATP:0000335"], \
                "data_novelty must keep resolving"
            assert helpers.atp_get_all_ancestors("ATP:0000325") == [
                "ATP:0000324", "ATP:0000323"], \
                "data_context ancestors must resolve from the default start terms"
        finally:
            helpers._get_atp_children, helpers._fetch_atp_names = real_children, real_names
            helpers.atp_to_parent.clear()
            helpers.atp_to_children.clear()
