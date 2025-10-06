from typing import Dict
from sqlalchemy import Column, Integer, String, ForeignKey, CheckConstraint, Index
from sqlalchemy.orm import relationship

from agr_literature_service.api.database.base import Base


class UserModel(Base):
    __tablename__ = "users"
    __versioned__: Dict = {}

    user_id = Column(Integer, primary_key=True, autoincrement=True)

    # legacy string id (Okta id / script name / "default_user") — kept temporarily
    id = Column(String, nullable=True, index=True, unique=True)

    automation_username = Column(String, nullable=True, index=True)

    person_id = Column(Integer, ForeignKey("person.person_id", ondelete="SET NULL"), nullable=True, index=True)
    person = relationship("PersonModel", back_populates="users")

    # (optional) back-compat only; plan to remove once callers migrate
    email = Column(String, nullable=True, index=True)

    __table_args__ = (
        CheckConstraint(
            "(person_id IS NULL) <> (automation_username IS NULL)",
            name="ck_users_exactly_one_of_person_or_automation",
        ),
        Index("ix_users_person_id", "person_id"),
    )

    def __str__(self) -> str:
        who = self.automation_username or (self.person.display_name if self.person else None) or self.id or "unknown"
        return f"User<{who}>"
