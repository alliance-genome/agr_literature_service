from sqlalchemy.orm import Session

from agr_literature_service.api.models.user_model import AutomationModel


def create(db: Session, user_id: str) -> AutomationModel:
    """Create an automation agent labelled user_id."""
    user_obj = AutomationModel(display_name=user_id)
    db.add(user_obj)
    db.commit()
    db.refresh(user_obj)

    return user_obj
