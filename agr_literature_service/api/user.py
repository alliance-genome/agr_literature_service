from contextvars import ContextVar
from typing import Optional, Dict, Any
from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

from agr_literature_service.api.crud import user_crud
from agr_literature_service.api.models.user_model import UserModel

# String primary key (users.id) of the "current user" for this request.
# A ContextVar isolates the value per asyncio task and per FastAPI threadpool
# worker, so concurrent requests cannot overwrite each other's identity before
# the SQLAlchemy `before_update` listener stamps `updated_by`.
_current_user_id: ContextVar[Optional[str]] = ContextVar(
    "_current_user_id", default=None
)


def _ensure_automation_user(db: Session, program_name: str) -> UserModel:
    """
    Ensure an automation/system user exists:
      users.id = program_name (string PK)
      users.automation_username = program_name
      users.person_id = NULL
    This satisfies the CHECK: (person_id IS NULL) <> (automation_username IS NULL)
    """
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
        raise HTTPException(
            status_code=403,
            detail=f"No user account linked to email address: {user_email}. "
                   "Contact an administrator to create your user account."
        )

    # Set the current user ID from the query result
    _current_user_id.set(result[0])


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
