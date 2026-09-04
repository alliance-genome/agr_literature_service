"""
Cognito Observer role support (SCRUM-6431 / SCRUM-6429).

An observer is a MOD-sponsored collaborator with READ-ONLY access to the ABC:
they can sign in, search, view reference data, and download open-access plus
their sponsoring MOD's restricted full text — but cannot create, update or
delete anything.

Design notes (from the ticket's repository review):

* Observer groups are resolved through an explicit group -> MOD mapping, not a
  string-prefix convention. Adding a MOD is a one-line mapping change.
* Observer membership is deliberately NOT mapped to ``ModAccess`` (the value
  ``agr_cognito_py.get_mod_access`` returns): that value grants mutation
  capability (file deletion, tag deletion). Content VISIBILITY and MUTATION
  capability are separated: ``visibility_mod_access`` widens what an observer
  can download, while ``get_mod_access`` still reports NO_ACCESS so nothing
  write-capable changes.
* A user who also holds any write-capable group (curator/admin/developer) or a
  service-account token is NOT an observer — the stronger role wins and
  existing behavior is unchanged.
* Enforcement is server-side in the shared authentication dependency
  (``IPAwareCognitoAuth``): every mutating HTTP method is rejected for
  observers with 403, regardless of what the UI exposes.
"""
import logging
from typing import Any, Dict, Optional, Set

from fastapi import HTTPException

from agr_cognito_py import ModAccess, get_mod_access

logger = logging.getLogger(__name__)

# Explicit observer group -> sponsoring MOD abbreviation. Group names follow the
# <CognitoModName>Observer convention agreed on SCRUM-6429 (FlyBaseObserver is
# the first provisioned group); the MOD abbreviations match mod.abbreviation.
OBSERVER_GROUP_TO_MOD: Dict[str, str] = {
    "SGDObserver": "SGD",
    "RGDObserver": "RGD",
    "MGIObserver": "MGI",
    "ZFINObserver": "ZFIN",
    "XenbaseObserver": "XB",
    "FlyBaseObserver": "FB",
    "WormBaseObserver": "WB",
}

# MOD abbreviation -> the ModAccess value used ONLY for content visibility
# (restricted full-text downloads). Never returned from get_mod_access, so it
# cannot leak into mutation paths.
_MOD_ABBR_TO_ACCESS: Dict[str, ModAccess] = {
    "SGD": ModAccess.SGD,
    "RGD": ModAccess.RGD,
    "MGI": ModAccess.MGI,
    "ZFIN": ModAccess.ZFIN,
    "XB": ModAccess.XB,
    "FB": ModAccess.FB,
    "WB": ModAccess.WB,
}

# HTTP methods an observer may use.
OBSERVER_ALLOWED_METHODS: Set[str] = {"GET", "HEAD", "OPTIONS"}

# Read-only POST endpoints (data retrieval / session management that happen to
# use POST bodies). Everything else with a mutating method is rejected for
# observers. Paths are compared with trailing slashes stripped.
OBSERVER_ALLOWED_POST_PATHS: Set[str] = {
    "/auth/login",
    "/auth/logout",
    "/search/references",
    "/ontology/term_details",
    "/cross_reference/show_all",
    "/topic_entity_tag/by_references",
    "/reference/referencefile/show_main_pdf_ids_for_curies",
    # Stateless converters (upload in, converted document out; nothing stored).
    "/xml2md/convert",
    "/xml2md/validate",
}


def observer_mod(user: Optional[Dict[str, Any]]) -> Optional[str]:
    """Return the sponsoring MOD abbreviation when ``user`` is an observer,
    else None.

    A user is an observer only when they carry an observer group AND hold no
    write-capable access: any curator/admin/developer group or a service-account
    (access) token supersedes observer membership, so granting an observer group
    to an existing curator changes nothing.

    Access tokens reaching this code are genuine service accounts: user-session
    access tokens (whose real groups agr_cognito_py discards) are rejected
    up-front by auth.reject_user_session_access_tokens, so the token_type check
    below is documentation/defense rather than the boundary itself.
    """
    if not user:
        return None
    if user.get("token_type") == "access":
        return None
    groups = user.get("cognito:groups") or []
    mods = [OBSERVER_GROUP_TO_MOD[g] for g in groups if g in OBSERVER_GROUP_TO_MOD]
    if not mods:
        return None
    if get_mod_access(user) != ModAccess.NO_ACCESS:
        return None
    if len(set(mods)) > 1:
        # Multiple sponsoring MODs is not part of the agreed model; take the
        # first deterministically and flag it for follow-up.
        logger.warning("User carries multiple observer groups %s; using %s",
                       sorted(set(mods)), mods[0])
    return mods[0]


def is_observer(user: Optional[Dict[str, Any]]) -> bool:
    """True when the user's effective role is observer (read-only)."""
    return observer_mod(user) is not None


def visibility_mod_access(user: Optional[Dict[str, Any]]) -> ModAccess:
    """MOD access level for CONTENT VISIBILITY decisions (restricted full-text
    and derived-file downloads). For an observer this is the sponsoring MOD's
    access value, so same-MOD restricted files are downloadable while other
    MODs' restricted files remain hidden; for everyone else it is exactly
    ``get_mod_access`` — mutation paths must keep calling ``get_mod_access``
    directly."""
    mod = observer_mod(user)
    if mod is not None:
        return _MOD_ABBR_TO_ACCESS[mod]
    return get_mod_access(user) if user else ModAccess.NO_ACCESS


def enforce_observer_read_only(user: Optional[Dict[str, Any]], method: str,
                               path: str) -> None:
    """Reject mutating requests from observers with 403.

    Called from the shared authentication dependency for every authenticated
    request, so the API refuses observer writes regardless of UI behavior
    (direct POST/PUT/PATCH/DELETE included). Read-only POST endpoints
    (search, lookups, session login/logout) are allowlisted explicitly.
    """
    if not is_observer(user):
        return
    if method in OBSERVER_ALLOWED_METHODS:
        return
    if method == "POST" and path.rstrip("/") in OBSERVER_ALLOWED_POST_PATHS:
        return
    raise HTTPException(
        status_code=403,
        detail=(
            "Your observer role is read-only: this operation is not permitted. "
            f"({method} {path})"
        ),
    )
