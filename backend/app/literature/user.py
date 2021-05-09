from fastapi_sqlalchemy import db

from literature.crud import user
from literature.models.user import User


user_id = None

def set_global_user_id(id: str):
    global user_id
    user_id = id

    if not db.session.query(User).filter(User.id == user_id).first():
        user.create(user_id)

def get_global_user_id():
    return user_id
