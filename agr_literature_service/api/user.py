from typing import Optional

from fastapi_okta import OktaUser
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
    u = db.query(UserModel).filter_by(id=program_name).one_or_none()
    if u is None:
        # Prefer CRUD if it accepts these keyword args; otherwise fall back to raw ORM.
        try:
            u = user_crud.create(
                db,
                id=program_name,
                email=None,
                automation_username=program_name,
                person_id=None,
            )
        except TypeError:
            u = UserModel(id=program_name, automation_username=program_name)
            db.add(u)
            db.commit()
            db.refresh(u)
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


def set_global_user_from_okta(db: Session, user: OktaUser) -> None:
    """
    Ensure a user row for this Okta principal.
    Until a Person is linked, keep automation_username = uid (and person_id = NULL)
    to satisfy the CHECK. When you later link a Person, set automation_username = NULL.
    """
    global _current_user_id
    uid: str = user.uid if user.uid else user.cid
    _current_user_id = uid

    user_email: Optional[str] = None
    if user.email and user.email != uid and "@" in user.email:
        user_email = user.email

    u = db.query(UserModel).filter_by(id=uid).one_or_none()
    if u is None:
        try:
            u = user_crud.create(
                db,
                id=uid,
                email=user_email,
                automation_username=uid,   # temp until person is linked
                person_id=None,
            )
        except TypeError:
            u = UserModel(id=uid, automation_username=uid, email=user_email)
            db.add(u)
            db.commit()
            db.refresh(u)
        return

    updated = False

    # Keep email synced
    if u.email != user_email:
        u.email = user_email
        updated = True

    # If neither side is set, set automation side to satisfy CHECK
    if u.person_id is None and u.automation_username is None:
        u.automation_username = uid
        updated = True

    # If already person-linked, clear automation_username
    if u.person_id is not None and u.automation_username is not None:
        u.automation_username = None
        updated = True

    if updated:
        db.add(u)
        db.commit()
        db.refresh(u)


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
    u = db.query(UserModel).filter_by(id=user_id_str).one_or_none()
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
