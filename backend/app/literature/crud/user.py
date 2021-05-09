from fastapi_sqlalchemy import db

from literature.models.user import User


def create(user_id):
    user_obj = User(id=user_id)
    db.session.add(user_obj)
    db.session.commit()
    db.session.refresh(user_obj)

    return user_obj
