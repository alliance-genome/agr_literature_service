"""SCRUM-6853: MOD access on topic entity tag writes, and the audit fields a
service account sets to record a write for a named curator. Wiring only:
authentication is overridden with a fake Cognito user, the DB session is a mock,
and the crud calls are patched, so these check who may call what and which
identity the write runs under."""
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException, status
from starlette.testclient import TestClient

from agr_literature_service.api import database
from agr_literature_service.api.auth import DEFAULT_BYPASS_USER, get_authenticated_user, is_service_client_token
from agr_literature_service.api.main import app
from agr_literature_service.api.routers import topic_entity_tag_router as router
from agr_literature_service.api.user import get_global_user_id

CURATOR_AGENT = (42, "AGRKB:101000000000042")
EDITOR_AGENT = (43, "AGRKB:101000000000043")

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
BYPASS = dict(DEFAULT_BYPASS_USER)


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
        _curator("FlyBaseCurator"), _curator("AllianceDeveloper"), SERVICE, BYPASS])
    def test_allowed(self, user):
        router.require_mod_write_access(user, "FB")

    @pytest.mark.parametrize("user", [_curator("WormBaseCurator"), _curator(), None])
    def test_other_mod_no_mod_or_no_user_is_403(self, user):
        with pytest.raises(HTTPException) as exc:
            router.require_mod_write_access(user, "FB")
        assert exc.value.status_code == status.HTTP_403_FORBIDDEN

    @pytest.mark.parametrize("mod", ["FB", "WB"])
    def test_a_curator_of_two_mods_writes_both(self, mod):
        # get_mod_access stops at the first curator group; the write check must not
        router.require_mod_write_access(_curator("FlyBaseCurator", "WormBaseCurator"), mod)

    def test_a_service_client_limited_to_one_mod_writes_only_that_mod(self, monkeypatch):
        monkeypatch.setenv("COGNITO_SERVICE_CLIENT_MODS", "service-client:WB")
        router.require_mod_write_access(SERVICE, "WB")
        with pytest.raises(HTTPException) as exc:
            router.require_mod_write_access(SERVICE, "FB")
        assert exc.value.status_code == status.HTTP_403_FORBIDDEN


class TestServiceClientToken:

    def test_a_client_credentials_token_is_a_service_client(self):
        assert is_service_client_token(SERVICE)

    @pytest.mark.parametrize("user", [
        BYPASS,  # token_type "access" but no client: not a credential at all
        _curator("FlyBaseCurator"),
        {"token_type": "access", "sub": "a-user-uuid", "client_id": "ui-client"},
        {"token_type": "access", "sub": "service-client", "client_id": "service-client",
         "scope": "aws.cognito.signin.user.admin"},
        None])
    def test_anything_else_is_not(self, user):
        assert not is_service_client_token(user)

    def test_the_allowlist_applies_when_set(self, monkeypatch):
        monkeypatch.setenv("COGNITO_SERVICE_CLIENT_IDS", "another-client")
        assert not is_service_client_token(SERVICE)
        monkeypatch.setenv("COGNITO_SERVICE_CLIENT_IDS", "another-client, service-client")
        assert is_service_client_token(SERVICE)


class TestCreate:

    def _post(self, client, user, body, source_mod="FB"):
        _as(user)
        with patch.object(router, "_source_mod", return_value=source_mod), \
                patch.object(router.topic_entity_tag_crud, "create_tag", return_value=(1, False)) as create, \
                patch.object(router.topic_entity_tag_crud, "show_tag", return_value=SHOWN), \
                patch.object(router, "resolve_named_person", return_value=CURATOR_AGENT):
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

    def test_person_created_by_is_resolved_strictly(self, client):
        # An AGRKB ID or email in created_by must name exactly one person; it is
        # stored as that person's label.
        _as(SERVICE)
        with patch.object(router, "_source_mod", return_value="FB"), \
                patch.object(router.topic_entity_tag_crud, "create_tag", return_value=(1, False)) as create, \
                patch.object(router.topic_entity_tag_crud, "show_tag", return_value=SHOWN), \
                patch.object(router, "resolve_named_person", return_value=CURATOR_AGENT) as resolve:
            response = client.post("/topic_entity_tag/", json={**TAG, "created_by": "curator@example.org"})
        assert response.status_code == status.HTTP_201_CREATED
        assert resolve.call_args.args[0] == "curator@example.org"
        assert create.call_args.args[1].created_by == CURATOR_AGENT[1]

    def test_unknown_person_created_by_is_422(self, client):
        _as(SERVICE)
        with patch.object(router, "_source_mod", return_value="FB"), \
                patch.object(router.topic_entity_tag_crud, "create_tag") as create, \
                patch.object(router, "resolve_named_person",
                             side_effect=HTTPException(status_code=422, detail="Unknown updated_by")):
            response = client.post("/topic_entity_tag/", json={**TAG, "created_by": "AGRKB:101000000000999"})
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
        create.assert_not_called()

    def test_lowercase_curie_is_resolved_strictly_too(self, client):
        _as(SERVICE)
        with patch.object(router, "_source_mod", return_value="FB"), \
                patch.object(router.topic_entity_tag_crud, "create_tag") as create, \
                patch.object(router, "resolve_named_person",
                             side_effect=HTTPException(status_code=422, detail="Unknown created_by")) as resolve:
            response = client.post("/topic_entity_tag/", json={**TAG, "created_by": "agrkb:101000000000999"})
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
        assert resolve.call_args.args[2] == "created_by"
        create.assert_not_called()

    def test_pipeline_label_created_by_is_left_alone(self, client):
        # Not a person identifier: the old behaviour (an automation agent) stays.
        response, create = self._post(client, SERVICE, {**TAG, "created_by": "some_classification_pipeline"})
        assert response.status_code == status.HTTP_201_CREATED
        assert create.call_args.args[1].created_by == "some_classification_pipeline"


class TestValidate:

    def test_curator_of_another_mod_is_403(self, client):
        _as(_curator("WormBaseCurator"))
        with patch.object(router.topic_entity_tag_crud, "validate_topic") as validate:
            response = client.post("/topic_entity_tag/validate", json=VERDICT)
        assert response.status_code == status.HTTP_403_FORBIDDEN
        validate.assert_not_called()

    def test_curator_of_two_mods_may_validate_for_the_second(self, client):
        _as(_curator("WormBaseCurator", "FlyBaseCurator"))
        with patch.object(router.topic_entity_tag_crud, "validate_topic", return_value={}) as validate:
            response = client.post("/topic_entity_tag/validate", json=VERDICT)
        assert response.status_code == status.HTTP_200_OK
        validate.assert_called_once()

    def test_curator_of_the_mod_may_validate(self, client):
        _as(_curator("FlyBaseCurator"))
        with patch.object(router.topic_entity_tag_crud, "validate_topic", return_value={}) as validate:
            response = client.post("/topic_entity_tag/validate", json=VERDICT)
        assert response.status_code == status.HTTP_200_OK
        validate.assert_called_once()

    def _validate(self, client, user, body):
        recorded = {}

        def capture(db, **kwargs):
            recorded["user"] = get_global_user_id()
            recorded.update(kwargs)
            return {}

        _as(user)
        agents = {"AGRKB:101000000000042": CURATOR_AGENT, "AGRKB:101000000000043": EDITOR_AGENT}

        def resolve(name, db, field):
            if name not in agents:  # resolve_named_person refuses automation agents
                raise HTTPException(status_code=422, detail=f"{field} {name!r} names an automation agent")
            return agents[name]

        with patch.object(router.topic_entity_tag_crud, "validate_topic", side_effect=capture) as validate, \
                patch.object(router, "resolve_named_person", side_effect=resolve):
            response = client.post("/topic_entity_tag/validate", json={**VERDICT, **body})
        return response, validate, recorded

    def test_service_account_records_the_verdict_for_a_curator(self, client):
        # Every service token is default_user; created_by gives each curator their
        # own verdict, and one audit user alone stands for both creator and editor.
        response, _, recorded = self._validate(client, SERVICE, {"created_by": "AGRKB:101000000000042"})
        assert response.status_code == status.HTTP_200_OK
        assert recorded["created_by"] == CURATOR_AGENT[1]
        assert recorded["user"] == CURATOR_AGENT[1]

    def test_creator_and_editor_can_differ(self, client):
        response, _, recorded = self._validate(client, SERVICE, {
            "created_by": "AGRKB:101000000000042", "updated_by": "AGRKB:101000000000043",
            "date_created": "2026-11-01T10:00:00", "date_updated": "2026-11-12T09:30:00"})
        assert response.status_code == status.HTTP_200_OK
        assert recorded["created_by"] == CURATOR_AGENT[1]
        assert recorded["user"] == EDITOR_AGENT[1]
        assert recorded["date_created"].isoformat() == "2026-11-01T10:00:00"
        assert recorded["date_updated"].isoformat() == "2026-11-12T09:30:00"

    def test_an_automation_agent_is_not_a_curator(self, client):
        # default_user (or a pipeline label) would bring back the shared owner.
        response, validate, _ = self._validate(client, SERVICE, {"created_by": "default_user"})
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
        validate.assert_not_called()

    def test_dates_with_an_offset_are_stored_as_utc(self, client):
        response, _, recorded = self._validate(client, SERVICE, {
            "created_by": "AGRKB:101000000000042", "date_updated": "2026-11-12T09:30:00-05:00"})
        assert response.status_code == status.HTTP_200_OK
        assert recorded["date_updated"].isoformat() == "2026-11-12T14:30:00"

    def test_service_account_without_a_named_curator_is_422(self, client):
        # The token alone would record the verdict as default_user, shared by every
        # service-account verdict.
        response, validate, _ = self._validate(client, SERVICE, {})
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
        validate.assert_not_called()

    def test_updated_by_alone_names_owner_and_editor(self, client):
        response, _, recorded = self._validate(client, SERVICE, {"updated_by": "AGRKB:101000000000043"})
        assert response.status_code == status.HTTP_200_OK
        assert recorded["created_by"] == EDITOR_AGENT[1]
        assert recorded["user"] == EDITOR_AGENT[1]

    def test_a_trusted_ip_without_a_token_may_name_the_curator(self, client):
        # A bypass IP can already write any tag without a token (SCRUM-6853 review)
        response, _, recorded = self._validate(client, BYPASS, {"created_by": "AGRKB:101000000000042"})
        assert response.status_code == status.HTTP_200_OK
        assert recorded["created_by"] == CURATOR_AGENT[1]
        assert recorded["user"] == CURATOR_AGENT[1]

    def test_a_trusted_ip_without_a_token_need_not_name_a_curator(self, client):
        # Not a service client, so no 422: the verdict is default_user's, as before
        response, validate, recorded = self._validate(client, BYPASS, {})
        assert response.status_code == status.HTTP_200_OK
        validate.assert_called_once()
        assert recorded["created_by"] is None

    @pytest.mark.parametrize("body", [{"created_by": "AGRKB:101000000000042"},
                                      {"date_updated": "2026-11-12T09:30:00"}])
    def test_a_curator_login_cannot_set_audit_fields(self, client, body):
        response, validate, _ = self._validate(client, _curator("FlyBaseCurator"), body)
        assert response.status_code == status.HTTP_403_FORBIDDEN
        validate.assert_not_called()


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


class TestDelete:

    def test_curator_of_another_mod_cannot_delete_a_tag(self, client):
        _as(_curator("WormBaseCurator"))
        tag = MagicMock()
        tag.tag_source.secondary_data_provider.abbreviation = "FB"
        db = MagicMock()
        db.get.return_value = tag
        app.dependency_overrides[database.get_db] = lambda: db
        with patch.object(router.topic_entity_tag_crud, "destroy_tag") as destroy:
            response = client.delete("/topic_entity_tag/5")
        assert response.status_code == status.HTTP_403_FORBIDDEN
        destroy.assert_not_called()

    def test_curator_of_another_mod_cannot_delete_its_manual_tags(self, client):
        _as(_curator("WormBaseCurator"))
        with patch.object(router.topic_entity_tag_utils, "delete_manual_tets") as delete_manual:
            response = client.delete("/topic_entity_tag/delete_manual_tets/AGRKB:101000000000001/FB")
        assert response.status_code == status.HTTP_403_FORBIDDEN
        delete_manual.assert_not_called()

    def test_curator_of_the_mod_may_delete_its_manual_tags(self, client):
        _as(_curator("FlyBaseCurator"))
        with patch.object(router.topic_entity_tag_utils, "delete_manual_tets") as delete_manual:
            response = client.delete("/topic_entity_tag/delete_manual_tets/AGRKB:101000000000001/FB")
        assert response.status_code == status.HTTP_204_NO_CONTENT
        delete_manual.assert_called_once()


class TestNoTetStatus:

    def test_curator_of_another_mod_is_403(self, client):
        _as(_curator("WormBaseCurator"))
        with patch.object(router.topic_entity_tag_crud, "set_indexing_status_for_no_tet_data") as set_status:
            response = client.post("/topic_entity_tag/set_no_tet_status/FB/AGRKB:101000000000001/some-uid")
        assert response.status_code == status.HTTP_403_FORBIDDEN
        set_status.assert_not_called()

    def test_curator_of_the_mod_may_set_it(self, client):
        _as(_curator("FlyBaseCurator"))
        with patch.object(router.topic_entity_tag_crud, "set_indexing_status_for_no_tet_data",
                          return_value={}) as set_status:
            response = client.post("/topic_entity_tag/set_no_tet_status/FB/AGRKB:101000000000001/some-uid")
        assert response.status_code == status.HTTP_200_OK
        set_status.assert_called_once()
