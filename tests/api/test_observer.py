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
