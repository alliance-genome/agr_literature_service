"""Unit tests for the Cognito Observer role (SCRUM-6431).

Pure tests over the observer module: role/MOD resolution for multiple MOD
observer groups, precedence of write-capable roles, the read-only enforcement
matrix (mutating methods rejected, GETs and allowlisted read-only POSTs
permitted), the visibility-vs-mutation access split, and the auto-registration
role gate. No DB, network, or Cognito access is needed.
"""
import pytest
from fastapi import HTTPException

from agr_cognito_py import ModAccess, get_mod_access

from agr_literature_service.api.observer import (
    OBSERVER_ALLOWED_POST_PATHS,
    OBSERVER_GROUP_TO_MOD,
    enforce_observer_read_only,
    is_observer,
    observer_mod,
    visibility_mod_access,
)
from agr_literature_service.api.user import _has_recognized_role_group


def _id_user(*groups):
    return {"token_type": "id", "cognito:groups": list(groups),
            "email": "someone@example.org"}


class TestObserverResolution:
    def test_every_mod_observer_group_resolves(self):
        expected = {"SGDObserver": "SGD", "RGDObserver": "RGD",
                    "MGIObserver": "MGI", "ZFINObserver": "ZFIN",
                    "XenbaseObserver": "XB", "FlyBaseObserver": "FB",
                    "WormBaseObserver": "WB"}
        assert OBSERVER_GROUP_TO_MOD == expected
        for group, mod in expected.items():
            assert observer_mod(_id_user(group)) == mod
            assert is_observer(_id_user(group)) is True

    def test_non_observer_users(self):
        assert observer_mod(None) is None
        assert observer_mod({}) is None
        assert observer_mod(_id_user()) is None
        assert observer_mod(_id_user("FBStaff")) is None
        assert is_observer(_id_user("FlyBaseCurator")) is False

    def test_write_capable_roles_supersede_observer_membership(self):
        # Curator + observer -> curator wins, unchanged behavior.
        u = _id_user("FlyBaseObserver", "FlyBaseCurator")
        assert observer_mod(u) is None
        assert get_mod_access(u) == ModAccess.FB
        # Admin/developer + observer -> full access wins.
        for write_group in ("SuperAdmin", "AllianceDeveloper", "FlyBaseDeveloper"):
            u = _id_user("FlyBaseObserver", write_group)
            assert observer_mod(u) is None
            assert get_mod_access(u) == ModAccess.ALL_ACCESS

    def test_service_account_is_never_observer(self):
        u = {"token_type": "access", "cognito:groups": ["FlyBaseObserver"]}
        assert observer_mod(u) is None
        assert get_mod_access(u) == ModAccess.ALL_ACCESS

    def test_observer_keeps_mutation_access_at_no_access(self):
        # The core separation: observers gain no ModAccess (mutation capability),
        # only visibility (tested below).
        assert get_mod_access(_id_user("FlyBaseObserver")) == ModAccess.NO_ACCESS


class TestVisibilityModAccess:
    def test_observer_gets_sponsoring_mod_visibility(self):
        assert visibility_mod_access(_id_user("FlyBaseObserver")) == ModAccess.FB
        assert visibility_mod_access(_id_user("SGDObserver")) == ModAccess.SGD

    def test_cross_mod_visibility_matrix(self):
        # Same-MOD allowed, other-MOD denied: the FB observer's visibility value
        # is exactly FB, never ALL_ACCESS or another MOD.
        access = visibility_mod_access(_id_user("FlyBaseObserver"))
        assert access == ModAccess.FB
        assert access not in (ModAccess.ALL_ACCESS, ModAccess.WB, ModAccess.SGD)

    def test_non_observers_unchanged(self):
        assert visibility_mod_access(_id_user("WormBaseCurator")) == ModAccess.WB
        assert visibility_mod_access({"token_type": "access"}) == ModAccess.ALL_ACCESS
        assert visibility_mod_access(_id_user()) == ModAccess.NO_ACCESS
        assert visibility_mod_access(None) == ModAccess.NO_ACCESS


class TestReadOnlyEnforcement:
    OBSERVER = _id_user("FlyBaseObserver")

    def test_get_always_allowed(self):
        enforce_observer_read_only(self.OBSERVER, "GET", "/reference/AGRKB:1")
        enforce_observer_read_only(self.OBSERVER, "GET",
                                   "/reference/referencefile/download_file/1")

    @pytest.mark.parametrize("method,path", [
        ("POST", "/reference/"),
        ("POST", "/topic_entity_tag/"),
        ("POST", "/reference/merge/AGRKB:1/AGRKB:2"),
        ("POST", "/reference/referencefile/file_upload/"),
        ("POST", "/topic_entity_tag/validate"),
        ("POST", "/workflow_tag/transition_to_workflow_status"),
        ("PATCH", "/reference/AGRKB:1"),
        ("PUT", "/anything"),
        ("DELETE", "/reference/referencefile/123"),
        ("DELETE", "/topic_entity_tag/123"),
    ])
    def test_mutating_requests_rejected_with_403(self, method, path):
        with pytest.raises(HTTPException) as exc:
            enforce_observer_read_only(self.OBSERVER, method, path)
        assert exc.value.status_code == 403
        assert "read-only" in exc.value.detail

    def test_read_only_posts_allowlisted(self):
        for path in OBSERVER_ALLOWED_POST_PATHS:
            enforce_observer_read_only(self.OBSERVER, "POST", path)
            # Trailing slash must not defeat the allowlist.
            enforce_observer_read_only(self.OBSERVER, "POST", path + "/")

    def test_search_and_login_explicitly_allowed(self):
        assert "/search/references" in OBSERVER_ALLOWED_POST_PATHS
        assert "/auth/login" in OBSERVER_ALLOWED_POST_PATHS

    def test_non_observers_never_blocked(self):
        for user in (None, _id_user(), _id_user("FlyBaseCurator"),
                     _id_user("FlyBaseObserver", "FlyBaseCurator"),
                     {"token_type": "access"}):
            enforce_observer_read_only(user, "DELETE", "/reference/AGRKB:1")
            enforce_observer_read_only(user, "POST", "/topic_entity_tag/")


class TestUserSessionAccessTokenGate:
    """A user-pool ACCESS token masquerades as an ALL_ACCESS service account
    (agr_cognito_py discards its groups), which would let an observer sidestep
    the read-only boundary with their own browser token — so the auth layer
    rejects them outright (SCRUM-6431 review, finding 1)."""

    def test_user_session_access_token_rejected(self):
        from agr_literature_service.api.auth import reject_user_session_access_tokens
        user = {"token_type": "access", "client_id": "browserclient",
                "scope": "aws.cognito.signin.user.admin"}
        with pytest.raises(HTTPException) as exc:
            reject_user_session_access_tokens(user)
        assert exc.value.status_code == 401
        assert "ID token" in exc.value.detail

    def test_client_credentials_token_accepted(self):
        from agr_literature_service.api.auth import reject_user_session_access_tokens
        # client_credentials tokens have sub == client_id.
        reject_user_session_access_tokens(
            {"token_type": "access", "client_id": "m2m", "sub": "m2m", "scope": "abc/read"})
        reject_user_session_access_tokens(
            {"token_type": "access", "client_id": "m2m", "sub": "m2m"})

    def test_hosted_ui_access_token_rejected_via_sub_mismatch(self):
        # Hosted-UI (authorization-code) access tokens carry only the requested
        # OAuth scopes — no aws.cognito.signin.user.admin — but their sub is the
        # user's UUID, not the client_id (SCRUM-6431 review, finding B).
        from agr_literature_service.api.auth import reject_user_session_access_tokens
        user = {"token_type": "access", "client_id": "webclient",
                "sub": "1234-user-uuid", "scope": "openid email profile"}
        with pytest.raises(HTTPException) as exc:
            reject_user_session_access_tokens(user)
        assert exc.value.status_code == 401

    def test_id_tokens_unaffected(self):
        from agr_literature_service.api.auth import reject_user_session_access_tokens
        reject_user_session_access_tokens(_id_user("FlyBaseObserver"))
        reject_user_session_access_tokens(_id_user("FlyBaseCurator"))

    def test_optional_client_id_allowlist(self, monkeypatch):
        from agr_literature_service.api.auth import reject_user_session_access_tokens
        monkeypatch.setenv("COGNITO_SERVICE_CLIENT_IDS", "svc-one, svc-two")
        reject_user_session_access_tokens(
            {"token_type": "access", "client_id": "svc-one", "scope": "abc/read"})
        with pytest.raises(HTTPException) as exc:
            reject_user_session_access_tokens(
                {"token_type": "access", "client_id": "rogue", "scope": "abc/read"})
        assert exc.value.status_code == 401


class TestPassiveObserverRegistration:
    """The auth dependency registers observers passively (they never reach the
    mutating handlers where registration otherwise happens) — best-effort,
    cached, and never blocking the request (SCRUM-6431 review, finding C)."""

    def test_hook_only_fires_for_observers(self):
        from unittest.mock import patch
        from agr_literature_service.api.auth import IPAwareCognitoAuth
        with patch("agr_literature_service.api.user.ensure_cognito_user_registered") as reg:
            IPAwareCognitoAuth._ensure_observer_registered(_id_user("FlyBaseCurator"))
            reg.assert_not_called()
            IPAwareCognitoAuth._ensure_observer_registered(_id_user("FlyBaseObserver"))
            reg.assert_called_once()

    def test_registration_failure_never_blocks_the_request(self):
        from unittest.mock import MagicMock, patch
        from agr_literature_service.api import user as user_mod
        user_mod._registered_emails.discard("duncan@example.org")
        broken_session = MagicMock()
        broken_session.execute.side_effect = RuntimeError("db down")
        with patch("agr_literature_service.api.database.main.SessionLocal",
                   return_value=broken_session):
            # Must not raise.
            user_mod.ensure_cognito_user_registered(
                {"token_type": "id", "cognito:groups": ["FlyBaseObserver"],
                 "email": "duncan@example.org"})
        assert "duncan@example.org" not in user_mod._registered_emails
        broken_session.rollback.assert_called_once()
        broken_session.close.assert_called_once()

    def test_registered_email_cache_short_circuits(self):
        from unittest.mock import patch
        from agr_literature_service.api import user as user_mod
        user_mod._registered_emails.add("cached@example.org")
        try:
            with patch("agr_literature_service.api.database.main.SessionLocal") as sess:
                user_mod.ensure_cognito_user_registered(
                    {"token_type": "id", "cognito:groups": ["FlyBaseObserver"],
                     "email": "Cached@Example.org"})
                sess.assert_not_called()
        finally:
            user_mod._registered_emails.discard("cached@example.org")


class TestAutoRegistrationRoleGate:
    def test_recognized_groups(self):
        assert _has_recognized_role_group(_id_user("FlyBaseObserver")) is True
        assert _has_recognized_role_group(_id_user("WormBaseCurator")) is True
        assert _has_recognized_role_group(_id_user("SuperAdmin")) is True
        assert _has_recognized_role_group(_id_user("FlyBaseDeveloper")) is True

    def test_unrecognized_groups_keep_the_admin_contact_error(self):
        assert _has_recognized_role_group(_id_user()) is False
        assert _has_recognized_role_group(_id_user("FBStaff")) is False
        assert _has_recognized_role_group(_id_user("SomeRandomGroup")) is False
