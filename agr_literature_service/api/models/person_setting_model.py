from sqlalchemy import (
    Column, Integer, String, Boolean, ForeignKey, Index
)
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import JSONB
from agr_literature_service.api.database.base import Base
from agr_literature_service.api.models.audited_model import AuditedModel


class PersonSettingModel(Base, AuditedModel):
    __tablename__ = "person_setting"

    person_setting_id = Column(Integer, primary_key=True, autoincrement=True)

    person_id = Column(
        Integer,
        ForeignKey("person.person_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    component_name = Column(String(), nullable=False, index=True)
    setting_name = Column(String(), nullable=False)  # user_given_name

    # “Only one True per (person_id, component_name)” is enforced by a partial unique index (see Alembic)
    default_setting = Column(Boolean, nullable=False, default=False, server_default="false")

    json_settings = Column(JSONB, nullable=False, server_default="{}")

    # relationships
    person = relationship("PersonModel", back_populates="settings")

    __table_args__ = (
        Index("ix_person_setting_person_component", "person_id", "component_name"),
        Index(
            "uq_person_setting_one_default",
            "person_id",
            "component_name",
            unique=True,
            postgresql_where=(default_setting.is_(True)),
        ),
    )

    def __str__(self) -> str:
        return f"{self.person_id}:{self.component_name} [{self.setting_name}] (default={self.default_setting})"
