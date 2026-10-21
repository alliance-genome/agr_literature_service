import logging
import time
from contextvars import ContextVar
from typing import TYPE_CHECKING, Optional, Dict, Any, Set, Tuple
from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from agr_literature_service.api.models.user_model import UserModel

# NOTE: ``user_crud`` and ``UserModel`` are imported lazily inside the
# functions that need them. This module sits at the centre of an import
# cycle (models -> versioning/audited_model -> api.user -> crud -> models),
# so a module-level import of anything from crud/models makes ``api.user``
# unimportable as an entry point (e.g. from lit_processing scripts).

# An agent's label is its automation_username, or the curie of its person: the
# string the API shows as created_by / updated_by (the legacy users.id). Exact
# match, as users.id was; an automation name is unique.
_SQL_AGENT_BY_LABEL = text("""
    SELECT a.id
    FROM agent a
    LEFT JOIN person p ON p.id = a.person_id
    WHERE a.automation_username = :label OR p.curie = :label
    ORDER BY a.id
    LIMIT 1
""")

# Idempotent insert of an automation agent (person_id NULL satisfies the CHECK
# (person_id IS NULL) <> (automation_username IS NULL)). ON CONFLICT makes a
# racing insert of the same label a no-op; the caller then re-reads it.
_SQL_INSERT_AUTOMATION_AGENT = text("""
    INSERT INTO agent (automation_username, person_id)
    VALUES (:label, NULL)
    ON CONFLICT (automation_username) DO NOTHING
    RETURNING id
""")

# Label of the "current user" for this request, and its agent.id.
# A ContextVar isolates the value per asyncio task and per FastAPI threadpool
# worker, so concurrent requests cannot overwrite each other's identity before
# the SQLAlchemy `before_update` listener stamps `updated_by`.
_current_user_id: ContextVar[Optional[str]] = ContextVar(
    "_current_user_id", default=None
)
_current_agent_id: ContextVar[Optional[int]] = ContextVar(
    "_current_agent_id", default=None
)


def _set_current_user(label: Optional[str], agent_id: Optional[int]) -> None:
    _current_user_id.set(label)
    _current_agent_id.set(agent_id)


def resolve_agent_id(connection: Connection, label: str) -> int:
    """
    Return the id of the agent labelled ``label``, creating an automation agent
    for a label never seen before.

    Uses the in-flight Connection rather than a Session so it can run inside
    SQLAlchemy ``before_insert`` / ``before_update`` listeners (see
    ``AuditedModel``): the agent is created in the same transaction that flushes
    the audited row, so the created_by_id / updated_by_id foreign keys hold
    without an extra commit. This is what lets an admin-token "created by" /
    "updated by" name supplied to *any* write endpoint auto-create its agent.
    """
    agent_id = connection.execute(_SQL_AGENT_BY_LABEL, {"label": label}).scalar()
    if agent_id is None:
        agent_id = connection.execute(_SQL_INSERT_AUTOMATION_AGENT, {"label": label}).scalar()
    if agent_id is None:  # a concurrent transaction inserted it first
        agent_id = connection.execute(_SQL_AGENT_BY_LABEL, {"label": label}).scalar()
    return agent_id


def _ensure_automation_user(db: Session, program_name: str) -> "UserModel":
    """
    Return the agent labelled ``program_name``, creating it as an automation
    agent if needed (committed, so other sessions can reference it at once).
    """
    from agr_literature_service.api.models.user_model import UserModel

    agent_id = db.execute(_SQL_AGENT_BY_LABEL, {"label": program_name}).scalar()
    if agent_id is None:
        agent_id = resolve_agent_id(db.connection(), program_name)
        db.commit()
    return db.get(UserModel, agent_id)


def set_global_user_id(db: Session, id: str) -> None:
    """
    Set the current user for this request/context by label (e.g., script or
    program name). An unknown label becomes an automation agent.
    """
    _set_current_user(id, _ensure_automation_user(db, id).user_id)


def add_user_if_not_exists(db: Session, user_id: str) -> None:
    """
    Back-compat helper. New labels are created as automation agents.
    """
    _ensure_automation_user(db, user_id)


def set_global_user_from_cognito(db: Session, cognito_user: Optional[Dict[str, Any]]) -> None:
    """
    Set the current request's user from a Cognito token.

    For ID tokens (user login): Looks up user by email via email table join.
    For access tokens (service accounts): Uses 'default_user' and creates if needed.
    For None (VPN bypass): Sets current user to None (anonymous access).
    """
    # VPN bypass - no authenticated user (anonymous access)
    if cognito_user is None:
        _set_current_user(None, None)
        return

    # Check if this is a service account (access token from client_credentials flow)
    token_type = cognito_user.get("token_type")
    if token_type == "access":
        # Service account - use default_user
        set_global_user_id(db, "default_user")
        return

    # ID token - look up user by email
    user_email: Optional[str] = cognito_user.get("email", "")

    if not user_email:
        raise HTTPException(
            status_code=403,
            detail="Cognito user does not have an associated email address."
        )

    result = db.execute(_SQL_AGENT_BY_EMAIL, {"email": user_email}).fetchone()

    if result is None:
        # Auto-register first-time Cognito users that carry a recognized ABC
        # role group (curator/admin/developer/observer): link to a
        # pre-registered person with this email, or create the person, with
        # mod_roles recording their Cognito groups so the access status matches
        # Cognito (SCRUM-6431). Accounts without any recognized role keep the
        # explicit contact-an-administrator error, as does any registration
        # failure (e.g. the curie service being unavailable) — a clean 403
        # beats a 500 on someone's first login.
        if _has_recognized_role_group(cognito_user):
            try:
                agent_id, label = _register_cognito_user(db, cognito_user, user_email)
            except Exception:
                logger.exception("Auto-registration failed for %s", user_email)
                db.rollback()
                raise HTTPException(
                    status_code=403,
                    detail=f"Could not auto-register an account for {user_email}. "
                           "Contact an administrator to create your user account."
                )
            _set_current_user(label, agent_id)
            return
        raise HTTPException(
            status_code=403,
            detail=f"No user account linked to email address: {user_email}. "
                   "Contact an administrator to create your user account."
        )

    # Set the current user from the query result
    _set_current_user(result.label, result.id)


def _has_recognized_role_group(cognito_user: Dict[str, Any]) -> bool:
    """True when the user carries at least one Cognito group ABC understands
    (a MOD curator group, an admin/developer group, or a MOD observer group)."""
    from agr_cognito_py.cognito_permissions import ADMIN_GROUPS, GROUP_TO_ACCESS
    from agr_literature_service.api.observer import OBSERVER_GROUP_TO_MOD
    groups = cognito_user.get("cognito:groups") or []
    return any(
        g in GROUP_TO_ACCESS or g in ADMIN_GROUPS or g in OBSERVER_GROUP_TO_MOD
        or g.endswith("Developer")
        for g in groups
    )


# Agent of the person with this email address; its label is the person curie.
# Match case-insensitively because (a) person_email may store the original
# mixed-case address and (b) Cognito tokens often carry mixed case. The
# ix_person_email_lower_email_address functional index supports the lower()
# match.
_SQL_AGENT_BY_EMAIL = text("""
    SELECT a.id, p.curie AS label
    FROM agent a
    JOIN person p ON p.id = a.person_id
    JOIN person_email e ON e.person_id = p.id
    WHERE lower(e.email_address) = lower(:email)
    ORDER BY a.id
    LIMIT 1
""")

# Person already registered (e.g. by an admin ahead of the user's first login)
# but with no agent yet: link instead of duplicating.
_SQL_PERSON_BY_EMAIL = text("""
    SELECT p.id, p.curie
    FROM person p
    JOIN person_email e ON e.person_id = p.id
    WHERE lower(e.email_address) = lower(:email)
    ORDER BY p.id
    LIMIT 1
""")

# Person-backed agent; the CHECK constraint requires exactly one of
# person_id / automation_username. Racing registrations are serialized by the
# advisory lock in _register_cognito_user.
_SQL_INSERT_PERSON_AGENT = text("""
    INSERT INTO agent (person_id, automation_username)
    VALUES (:pid, NULL)
    RETURNING id
""")


def _register_cognito_user(db: Session, cognito_user: Dict[str, Any],
                           user_email: str) -> Tuple[int, str]:
    """Register a first-time Cognito login and return (agent id, label); the
    label is the person curie, the same shape manually-registered curators have.

    Concurrency: the UI fires many API calls in parallel right after login and
    each would otherwise pass the missing-user lookup and create a duplicate
    person. A transaction-scoped advisory lock on the email serializes them;
    both lookups are re-run under the lock, so every racer after the first
    resolves the freshly created rows instead of duplicating.

    Atomicity: person, email and agent rows commit together — a failure cannot
    leave a person without an agent, which would re-create duplicates on
    every later login.

    Models are imported lazily: this module sits at the centre of an import
    cycle (see the NOTE at the top of the file)."""
    from agr_literature_service.api.models import PersonEmailModel, PersonModel
    from agr_literature_service.global_utils import get_next_person_curie

    db.execute(text("SELECT pg_advisory_xact_lock(hashtext(lower(:email)))"),
               {"email": user_email})

    # Re-check now that we hold the lock: a concurrent request may have just
    # finished registering this user. Commit before returning — the advisory
    # lock is transaction-scoped and would otherwise be held for the rest of
    # the request, serializing the racers end-to-end instead of just past the
    # insert.
    row = db.execute(_SQL_AGENT_BY_EMAIL, {"email": user_email}).fetchone()
    if row:
        db.commit()
        return row.id, row.label

    groups = list(cognito_user.get("cognito:groups") or [])
    person_row = db.execute(_SQL_PERSON_BY_EMAIL, {"email": user_email}).fetchone()
    if person_row:
        person_id, curie = person_row[0], person_row[1]
        # Record the Cognito role status on a pre-registered person only when
        # the admin left it unset — never clobber curated roles.
        if groups:
            db.execute(text(
                "UPDATE person SET mod_roles = :groups "
                "WHERE id = :pid AND mod_roles IS NULL"
            ), {"groups": groups, "pid": person_id})
    else:
        display_name = (cognito_user.get("name")
                        or cognito_user.get("cognito:username")
                        or user_email)
        person = PersonModel(
            display_name=display_name,
            curie=get_next_person_curie(db),
            mod_roles=groups or None,
        )
        db.add(person)
        db.flush()
        db.add(PersonEmailModel(person_id=person.person_id,
                                email_address=user_email))
        db.flush()
        person_id, curie = person.person_id, person.curie

    agent_id = db.execute(_SQL_INSERT_PERSON_AGENT, {"pid": person_id}).scalar()
    db.commit()
    return agent_id, curie


def get_global_user_id() -> Optional[str]:
    """Return the current user's label, or None."""
    return _current_user_id.get()


def get_current_agent_id() -> Optional[int]:
    """Return the current user's agent.id, or None."""
    return _current_agent_id.get()


def get_current_user_pk(db: Session) -> Optional[int]:
    """
    Return the agent.id of the current user (creating the automation agent if
    necessary). Continuum's UserPlugin stores it in transaction.user_id.
    """
    agent_id = _current_agent_id.get()
    if agent_id is not None:
        return agent_id
    label = _current_user_id.get()
    if label is None:
        return None
    return _ensure_automation_user(db, label).user_id


def link_user_to_person(db: Session, user_id_str: str, person_id: int) -> None:
    """
    Switch the agent labelled ``user_id_str`` from 'automation mode' to
    'person-backed' mode: person_id = <id>, automation_username = NULL.
    This keeps the CHECK constraint valid. Its label becomes the person curie.
    """
    u = _ensure_automation_user(db, user_id_str)

    changed = False
    if u.person_id != person_id:
        u.person_id = person_id
        changed = True
    if u.automation_username is not None:
        u.automation_username = None
        changed = True

    if changed:
        db.add(u)
        db.commit()
        db.refresh(u)


# Emails already confirmed registered this process — makes the per-request
# observer registration hook a set lookup after first touch. Re-checked against
# the DB on process restart, and safe across workers: registration itself is
# advisory-locked and idempotent.
_registered_emails: Set[str] = set()

# email -> monotonic timestamp of the last FAILED registration attempt. A
# failure (e.g. the MATI curie service down) is negative-cached so an outage
# costs one attempt per cooldown instead of one blocking round trip per
# observer request (SCRUM-6431 review).
_registration_failed_at: Dict[str, float] = {}
_REGISTRATION_RETRY_SECONDS = 300


def ensure_cognito_user_registered(cognito_user: Dict[str, Any]) -> None:
    """Passively register a recognized-role Cognito user if they have no
    account yet (SCRUM-6431 review, finding C).

    set_global_user_from_cognito only runs inside mutating route handlers, and
    observers never reach those (the auth dependency 403s their writes first) —
    so without this hook an observer would never get a person row and their
    Cognito role status would never be recorded. Called from the auth
    dependency for observers (via the threadpool: registration can do
    synchronous curie-service HTTP); best-effort by design: a failure is
    logged, negative-cached for _REGISTRATION_RETRY_SECONDS, and never blocks
    the (read-only) request.
    """
    email = (cognito_user.get("email") or "").strip()
    key = email.lower()
    if not email or key in _registered_emails:
        return
    failed_at = _registration_failed_at.get(key)
    if failed_at is not None and time.monotonic() - failed_at < _REGISTRATION_RETRY_SECONDS:
        return
    if not _has_recognized_role_group(cognito_user):
        return
    db = None
    try:
        from agr_literature_service.api.database.main import SessionLocal
        db = SessionLocal()
        row = db.execute(_SQL_AGENT_BY_EMAIL, {"email": email}).fetchone()
        if row is None:
            _register_cognito_user(db, cognito_user, email)
            logger.info("Auto-registered Cognito user %s", email)
        _registered_emails.add(key)
        _registration_failed_at.pop(key, None)
    except Exception:
        logger.exception("Passive auto-registration failed for %s", email)
        _registration_failed_at[key] = time.monotonic()
        if db is not None:
            db.rollback()
    finally:
        if db is not None:
            db.close()
