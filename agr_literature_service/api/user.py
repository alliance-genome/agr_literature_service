from typing import Optional, Dict, Any
from sqlalchemy import text
from sqlalchemy.orm import Session

from agr_literature_service.api.crud import user_crud
from agr_literature_service.api.models.user_model import UserModel

# String primary key (users.id) of the "current user" for this process/request
_current_user_id: Optional[str] = None


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
        u = user_crud.create(db, program_name, None)
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
    Manually set the global users.id (e.g., script/program name).
    Treated as an automation user.
    """
    global _current_user_id
    _current_user_id = id
    _ensure_automation_user(db, id)


def add_user_if_not_exists(db: Session, user_id: str) -> None:
    """
    Back-compat helper. New IDs are treated as automation users.
    """
    _ensure_automation_user(db, user_id)


def set_global_user_from_cognito(db: Session, cognito_user: Optional[Dict[str, Any]]) -> None:
    """
    Set the global user from a Cognito token.

    For ID tokens (user login): Looks up user by email via email table join.
    For access tokens (service accounts): Uses 'default_user' and creates if needed.
    For None (VPN bypass): Sets current user to None (anonymous access).
    """
    global _current_user_id

    # VPN bypass - no authenticated user (anonymous access)
    if cognito_user is None:
        _current_user_id = None
        return

    # Check if this is a service account (access token from client_credentials flow)
    token_type = cognito_user.get("token_type")
    if token_type == "access":
        # Service account - use default_user
        default_user_id = "default_user"
        _current_user_id = default_user_id
        _ensure_automation_user(db, default_user_id)
        return

    # ID token - look up user by email
    user_email: Optional[str] = cognito_user.get("email", "")

    if not user_email:
        raise ValueError("Cognito user does not have an associated email address.")

    # Query using raw SQL to avoid circular import with EmailModel
    sql = text("""
        SELECT u.id
        FROM users u
        JOIN email e ON u.person_id = e.person_id
        WHERE e.email_address = :email
        ORDER BY u.id
        LIMIT 1
    """)

    result = db.execute(sql, {"email": user_email}).fetchone()

    if result is None:
        raise ValueError(f"No user found with email address: {user_email}")

    # Set the global user ID from the query result
    _current_user_id = result[0]


def get_global_user_id() -> Optional[str]:
    """Return the current users.id (string PK), or None."""
    return _current_user_id


def get_current_user_pk(db: Session) -> Optional[int]:
    """
    Return the integer users.user_id for the current user (creating the automation
    user if necessary). Use this when inserting into the `transaction` table.
    """
    if _current_user_id is None:
        return None
    u = _ensure_automation_user(db, _current_user_id)
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
