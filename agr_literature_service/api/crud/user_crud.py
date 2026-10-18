from sqlalchemy.orm import Session

from agr_literature_service.api.models.user_model import UserModel


def create(db: Session, user_id: str):
    """Create an automation agent labelled user_id."""
    user_obj = UserModel(automation_username=user_id, person_id=None)
    db.add(user_obj)
    db.commit()
    db.refresh(user_obj)

    return user_obj
