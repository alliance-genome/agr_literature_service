from agr_literature_service.api.database.base import Base
from agr_literature_service.api.models.audited_model import AuditedModel
from sqlalchemy import (Column, Integer, String)


class WorkflowTagTopicModel(AuditedModel, Base):
    __tablename__ = "workflow_tag_topic"

    workflow_tag_topic_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    workflow_tag = Column(
        String(),
        index=True,
        unique=True,
        nullable=False
    )

    # Obtained from A-Team ontology node term-id
    topic = Column(
        String(),
        unique=False,
        nullable=False
    )
