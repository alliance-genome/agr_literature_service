from contextvars import ContextVar
from typing import TYPE_CHECKING, Optional, Dict, Any
from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.orm import Session

if TYPE_CHECKING:
    from agr_literature_service.api.models.user_model import UserModel

# NOTE: ``user_crud`` and ``UserModel`` are imported lazily inside the
# functions that need them. This module sits at the centre of an import
# cycle (models -> versioning/audited_model -> api.user -> crud -> models),
# so a module-level import of anything from crud/models makes ``api.user``
# unimportable as an entry point (e.g. from lit_processing scripts).

# Idempotent insert of an automation user row. ``automation_username`` is set to
# the same value as ``id`` (with ``person_id`` NULL) to satisfy the table CHECK:
# (person_id IS NULL) <> (automation_username IS NULL). ON CONFLICT makes it a
# no-op when the user already exists, so it is safe to call on a hot path.
_SQL_INSERT_AUTOMATION_USER = text("""
    INSERT INTO users (id, automation_username, person_id)
    VALUES (:uid, :uid, NULL)
    ON CONFLICT (id) DO NOTHING
""")

# String primary key (users.id) of the "current user" for this request.
# A ContextVar isolates the value per asyncio task and per FastAPI threadpool
# worker, so concurrent requests cannot overwrite each other's identity before
# the SQLAlchemy `before_update` listener stamps `updated_by`.
_current_user_id: ContextVar[Optional[str]] = ContextVar(
    "_current_user_id", default=None
)


def _ensure_automation_user(db: Session, program_name: str) -> "UserModel":
    """
    Ensure an automation/system user exists:
      users.id = program_name (string PK)
      users.automation_username = program_name
      users.person_id = NULL
    This satisfies the CHECK: (person_id IS NULL) <> (automation_username IS NULL)
    """
    from agr_literature_service.api.crud import user_crud
    from agr_literature_service.api.models.user_model import UserModel

    # Use .first() instead of .one_or_none() to avoid MultipleResultsFound
    u = db.query(UserModel).filter_by(id=program_name).first()
    if u is None:
        # user_crud.create sets automation_username=<id>, person_id=NULL
        u = user_crud.create(db, program_name)
        return u

    # If it exists but both fields are NULL, set automation side to satisfy CHECK.
    if u.person_id is None and u.automation_username is None:
        u.automation_username = program_name
        db.add(u)
        db.commit()
        db.refresh(u)
    return u


def set_global_user_id(db: Session, id: str) -> None:
    """
    Set the current users.id for this request/context (e.g., script or
    program name). Treated as an automation user.
    """
    _current_user_id.set(id)
    _ensure_automation_user(db, id)


def add_user_if_not_exists(db: Session, user_id: str) -> None:
    """
    Back-compat helper. New IDs are treated as automation users.
    """
    _ensure_automation_user(db, user_id)


def ensure_user_exists_on_connection(connection: Connection, user_id: Optional[str]) -> None:
    """
    Idempotently create an automation ``users`` row for ``user_id`` using a raw
    Connection rather than a Session.

    This is the Connection-based counterpart to ``add_user_if_not_exists`` and is
    meant to be called from SQLAlchemy ``before_insert`` / ``before_update`` event
    listeners (see ``AuditedModel``), where only the in-flight Connection — not a
    Session — is available. Because it emits ``INSERT ... ON CONFLICT DO NOTHING``
    on the same Connection (and therefore the same transaction) that is flushing
    the audited row, the ``created_by`` / ``updated_by`` foreign keys are satisfied
    without an extra commit.

    This lets an admin-token "created by"/"updated by" name supplied to *any*
    write endpoint auto-create its ``users`` record instead of failing the FK.
    """
    if not user_id:
        return
    connection.execute(_SQL_INSERT_AUTOMATION_USER, {"uid": user_id})


def set_global_user_from_cognito(db: Session, cognito_user: Optional[Dict[str, Any]]) -> None:
    """
    Set the current request's user from a Cognito token.

    For ID tokens (user login): Looks up user by email via email table join.
    For access tokens (service accounts): Uses 'default_user' and creates if needed.
    For None (VPN bypass): Sets current user to None (anonymous access).
    """
    # VPN bypass - no authenticated user (anonymous access)
    if cognito_user is None:
        _current_user_id.set(None)
        return

    # Check if this is a service account (access token from client_credentials flow)
    token_type = cognito_user.get("token_type")
    if token_type == "access":
        # Service account - use default_user
        default_user_id = "default_user"
        _current_user_id.set(default_user_id)
        _ensure_automation_user(db, default_user_id)
        return

    # ID token - look up user by email
    user_email: Optional[str] = cognito_user.get("email", "")

    if not user_email:
        raise HTTPException(
            status_code=403,
            detail="Cognito user does not have an associated email address."
        )

    # Query using raw SQL to avoid circular import with PersonEmailModel.
    # Match case-insensitively because (a) person_email may store the
    # original mixed-case address and (b) Cognito tokens often carry
    # mixed case. The ix_person_email_lower_email_address functional
    # index supports the lower() match.
    sql = text("""
        SELECT u.id
        FROM users u
        JOIN person_email e ON u.person_id = e.person_id
        WHERE lower(e.email_address) = lower(:email)
        ORDER BY u.id
        LIMIT 1
    """)

    result = db.execute(sql, {"email": user_email}).fetchone()

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
                user_id = _register_cognito_user(db, cognito_user, user_email, sql)
            except Exception:
                db.rollback()
                raise HTTPException(
                    status_code=403,
                    detail=f"Could not auto-register an account for {user_email}. "
                           "Contact an administrator to create your user account."
                )
            _current_user_id.set(user_id)
            return
        raise HTTPException(
            status_code=403,
            detail=f"No user account linked to email address: {user_email}. "
                   "Contact an administrator to create your user account."
        )

    # Set the current user ID from the query result
    _current_user_id.set(result[0])


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


# Person already registered (e.g. by an admin ahead of the user's first login)
# but with no users row yet: link instead of duplicating.
_SQL_PERSON_BY_EMAIL = text("""
    SELECT p.person_id, p.curie
    FROM person p
    JOIN person_email e ON e.person_id = p.person_id
    WHERE lower(e.email_address) = lower(:email)
    ORDER BY p.person_id
    LIMIT 1
""")

# Person-backed users row; the CHECK constraint requires exactly one of
# person_id / automation_username. ON CONFLICT makes racing registrations safe.
_SQL_INSERT_PERSON_USER = text("""
    INSERT INTO users (id, person_id, automation_username)
    VALUES (:id, :pid, NULL)
    ON CONFLICT (id) DO NOTHING
""")


def _register_cognito_user(db: Session, cognito_user: Dict[str, Any],
                           user_email: str, user_lookup_sql) -> str:
    """Register a first-time Cognito login and return their users.id (the
    person curie — the same shape manually-registered person users have).

    Concurrency: the UI fires many API calls in parallel right after login and
    each would otherwise pass the missing-user lookup and create a duplicate
    person. A transaction-scoped advisory lock on the email serializes them;
    both lookups are re-run under the lock, so every racer after the first
    resolves the freshly created rows instead of duplicating.

    Atomicity: person, email and users rows commit together — a failure cannot
    leave a person without a users row, which would re-create duplicates on
    every later login.

    Models are imported lazily: this module sits at the centre of an import
    cycle (see the NOTE at the top of the file)."""
    from agr_literature_service.api.models import PersonEmailModel, PersonModel
    from agr_literature_service.global_utils import get_next_person_curie

    db.execute(text("SELECT pg_advisory_xact_lock(hashtext(lower(:email)))"),
               {"email": user_email})

    # Re-check now that we hold the lock: a concurrent request may have just
    # finished registering this user.
    row = db.execute(user_lookup_sql, {"email": user_email}).fetchone()
    if row:
        return row[0]

    groups = list(cognito_user.get("cognito:groups") or [])
    person_row = db.execute(_SQL_PERSON_BY_EMAIL, {"email": user_email}).fetchone()
    if person_row:
        person_id, curie = person_row[0], person_row[1]
        # Record the Cognito role status on a pre-registered person only when
        # the admin left it unset — never clobber curated roles.
        if groups:
            db.execute(text(
                "UPDATE person SET mod_roles = :groups "
                "WHERE person_id = :pid AND mod_roles IS NULL"
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

    db.execute(_SQL_INSERT_PERSON_USER, {"id": curie, "pid": person_id})
    db.commit()
    return curie


def get_global_user_id() -> Optional[str]:
    """Return the current users.id (string PK), or None."""
    return _current_user_id.get()


def get_current_user_pk(db: Session) -> Optional[int]:
    """
    Return the integer users.user_id for the current user (creating the automation
    user if necessary). Use this when inserting into the `transaction` table.
    """
    uid = _current_user_id.get()
    if uid is None:
        return None
    u = _ensure_automation_user(db, uid)
    return getattr(u, "user_id", None)


def link_user_to_person(db: Session, user_id_str: str, person_id: int) -> None:
    """
    Switch a user from 'automation mode' to 'person-backed' mode:
      person_id = <id>, automation_username = NULL
    This keeps the CHECK constraint valid.
    """
    from agr_literature_service.api.models.user_model import UserModel

    # Again, use .first() to be resilient to accidental duplicates.
    u = db.query(UserModel).filter_by(id=user_id_str).first()
    if u is None:
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
