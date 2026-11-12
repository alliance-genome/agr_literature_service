"""SCRUM-6853: MOD access on topic entity tag writes, and acting_as for service
accounts. Wiring only: authentication is overridden with a fake Cognito user, the
DB session is a mock, and the crud calls are patched, so these check who may call
what and which identity the write runs under."""
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException, status
from starlette.testclient import TestClient

from agr_literature_service.api import database
from agr_literature_service.api.auth import get_authenticated_user
from agr_literature_service.api.main import app
from agr_literature_service.api.routers import topic_entity_tag_router as router
from agr_literature_service.api.user import get_global_user_id

CURATOR_AGENT = (42, "AGRKB:101000000000042")

TAG = {
    "reference_curie": "AGRKB:101000000000001",
    "topic": "ATP:0000122",
    "tag_source_id": 7,
    "negated": False,
    "data_novelty": "ATP:0000334",
}
# what the patched show_tag returns: just enough for the TopicEntityTagSchemaShow response
SHOWN = {"topic_entity_tag_id": 1, "topic": "ATP:0000122", "tag_source_id": 7,
         "reference_curie": "AGRKB:101000000000001"}
VERDICT = {"reference_curie": "AGRKB:101000000000001", "topic": "ATP:0000122",
           "mod_abbreviation": "FB", "negated": False}


def _curator(*groups):
    return {"token_type": "id", "sub": "a-uuid", "email": "curator@example.org", "cognito:groups": list(groups)}


SERVICE = {"token_type": "access", "sub": "service-client", "client_id": "service-client"}


@pytest.fixture
def client():
    app.dependency_overrides[database.get_db] = lambda: MagicMock()
    with patch.object(router, "set_global_user_from_cognito"):
        yield TestClient(app)
    app.dependency_overrides.clear()


def _as(user):
    app.dependency_overrides[get_authenticated_user] = lambda: user


class TestRequireModWriteAccess:

    @pytest.mark.parametrize("user", [
        _curator("FlyBaseCurator"), _curator("AllianceDeveloper"), SERVICE, None])
    def test_allowed(self, user):
        router.require_mod_write_access(user, "FB")

    @pytest.mark.parametrize("user", [_curator("WormBaseCurator"), _curator()])
    def test_other_mod_or_no_mod_is_403(self, user):
        with pytest.raises(HTTPException) as exc:
            router.require_mod_write_access(user, "FB")
        assert exc.value.status_code == status.HTTP_403_FORBIDDEN


class TestCreate:

    def _post(self, client, user, body, source_mod="FB"):
        _as(user)
        with patch.object(router, "_source_mod", return_value=source_mod), \
                patch.object(router.topic_entity_tag_crud, "create_tag", return_value=(1, False)) as create, \
                patch.object(router.topic_entity_tag_crud, "show_tag", return_value=SHOWN), \
                patch.object(router, "resolve_named_agent", return_value=CURATOR_AGENT):
            response = client.post("/topic_entity_tag/", json=body)
        return response, create

    def test_curator_of_the_mod_may_create(self, client):
        response, create = self._post(client, _curator("FlyBaseCurator"), TAG)
        assert response.status_code == status.HTTP_201_CREATED
        create.assert_called_once()

    def test_curator_of_another_mod_is_403(self, client):
        response, create = self._post(client, _curator("WormBaseCurator"), TAG)
        assert response.status_code == status.HTTP_403_FORBIDDEN
        create.assert_not_called()

    def test_service_account_acting_as_a_curator(self, client):
        recorded = {}

        def capture(db, tag):
            recorded["user"] = get_global_user_id()
            recorded["acting_as_passed_on"] = "acting_as" in tag.model_dump()
            return (1, False)

        _as(SERVICE)
        with patch.object(router, "_source_mod", return_value="FB"), \
                patch.object(router.topic_entity_tag_crud, "create_tag", side_effect=capture), \
                patch.object(router.topic_entity_tag_crud, "show_tag", return_value=SHOWN), \
                patch.object(router, "resolve_named_agent", return_value=CURATOR_AGENT) as resolve:
            response = client.post("/topic_entity_tag/", json={**TAG, "acting_as": "curator@example.org"})
        assert response.status_code == status.HTTP_201_CREATED
        assert resolve.call_args.args[0] == "curator@example.org"
        assert recorded == {"user": CURATOR_AGENT[1], "acting_as_passed_on": False}

    def test_curator_cannot_act_as_someone_else(self, client):
        response, create = self._post(client, _curator("FlyBaseCurator"), {**TAG, "acting_as": "AGRKB:101000000000099"})
        assert response.status_code == status.HTTP_403_FORBIDDEN
        create.assert_not_called()

    def test_acting_as_with_created_by_is_422(self, client):
        response, create = self._post(client, SERVICE, {**TAG, "acting_as": "AGRKB:101000000000042",
                                                        "created_by": "AGRKB:101000000000099"})
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
        create.assert_not_called()

    def test_unresolvable_acting_as_is_422(self, client):
        _as(SERVICE)
        with patch.object(router, "_source_mod", return_value="FB"), \
                patch.object(router.topic_entity_tag_crud, "create_tag") as create, \
                patch.object(router, "resolve_named_agent",
                             side_effect=HTTPException(status_code=422, detail="Unknown updated_by 'nobody'")):
            response = client.post("/topic_entity_tag/", json={**TAG, "acting_as": "nobody"})
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
        create.assert_not_called()


class TestValidate:

    def test_curator_of_another_mod_is_403(self, client):
        _as(_curator("WormBaseCurator"))
        with patch.object(router.topic_entity_tag_crud, "validate_topic") as validate:
            response = client.post("/topic_entity_tag/validate", json=VERDICT)
        assert response.status_code == status.HTTP_403_FORBIDDEN
        validate.assert_not_called()

    def test_curator_of_the_mod_may_validate(self, client):
        _as(_curator("FlyBaseCurator"))
        with patch.object(router.topic_entity_tag_crud, "validate_topic", return_value={}) as validate:
            response = client.post("/topic_entity_tag/validate", json=VERDICT)
        assert response.status_code == status.HTTP_200_OK
        validate.assert_called_once()

    def test_service_account_acting_as_records_the_curator(self, client):
        # Every service token is default_user; acting_as gives each curator their own
        # identity, so the verdict lookup by creator finds that curator's verdict.
        recorded = {}

        def capture(db, **kwargs):
            recorded["user"] = get_global_user_id()
            return {}

        _as(SERVICE)
        with patch.object(router.topic_entity_tag_crud, "validate_topic", side_effect=capture), \
                patch.object(router, "resolve_named_agent", return_value=CURATOR_AGENT):
            response = client.post("/topic_entity_tag/validate",
                                   json={**VERDICT, "acting_as": "AGRKB:101000000000042"})
        assert response.status_code == status.HTTP_200_OK
        assert recorded["user"] == CURATOR_AGENT[1]


class TestPatch:

    def _patch(self, client, user, tag_mod="FB"):
        _as(user)
        tag = MagicMock()
        tag.tag_source.secondary_data_provider.abbreviation = tag_mod
        db = MagicMock()
        db.get.return_value = tag
        app.dependency_overrides[database.get_db] = lambda: db
        with patch.object(router.topic_entity_tag_crud, "patch_tag") as patch_tag, \
                patch.object(router.topic_entity_tag_crud, "show_tag", return_value=SHOWN):
            response = client.patch("/topic_entity_tag/5", json={"note": "edited"})
        return response, patch_tag

    def test_curator_of_another_mod_is_403(self, client):
        response, patch_tag = self._patch(client, _curator("WormBaseCurator"))
        assert response.status_code == status.HTTP_403_FORBIDDEN
        patch_tag.assert_not_called()

    def test_curator_of_the_mod_may_patch(self, client):
        response, patch_tag = self._patch(client, _curator("FlyBaseCurator"))
        assert response.status_code == status.HTTP_200_OK
        patch_tag.assert_called_once()
