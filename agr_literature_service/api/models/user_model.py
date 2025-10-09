from typing import Dict
from sqlalchemy import Column, Integer, String, ForeignKey, CheckConstraint
from sqlalchemy.orm import relationship
from agr_literature_service.api.database.base import Base


class UserModel(Base):
    __tablename__ = "users"

    user_id = Column(Integer, primary_key=True, autoincrement=True)
    id = Column(String, nullable=True, index=True, unique=True)  # legacy
    automation_username = Column(String, nullable=True, index=True)

    person_id = Column(Integer, ForeignKey("person.person_id", ondelete="SET NULL"), nullable=True, index=True)

    # Relationship to PersonModel
    person = relationship(
        "PersonModel",
        foreign_keys=[person_id]
    )

    email = Column(String, nullable=True, index=True)

    __table_args__ = (
        CheckConstraint(
            "(person_id IS NULL) <> (automation_username IS NULL)",
            name="ck_users_exactly_one_of_person_or_automation"
        ),
    )

    def __str__(self) -> str:
        who = self.automation_username or (self.person.display_name if self.person else None) or self.id or "unknown"
        return f"User<{who}>"
