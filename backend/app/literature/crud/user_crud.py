from sqlalchemy.orm import Session

from literature.models.user import User


def create(db: Session, user_id: str):
    user_obj = User(id=user_id)
    db.add(user_obj)
    db.commit()
    db.refresh(user_obj)

    return user_obj
