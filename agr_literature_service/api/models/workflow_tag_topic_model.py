from agr_literature_service.api.database.base import Base
from agr_literature_service.api.models.audited_model import AuditedModel
from agr_literature_service.api.models.ontology_term_model import ontology_curie, register_curie_columns
from sqlalchemy import (BigInteger, Column, ForeignKey, Integer)


class WorkflowTagTopicModel(AuditedModel, Base):
    __tablename__ = "workflow_tag_topic"

    workflow_tag_topic_id = Column(
        'id',
        Integer,
        primary_key=True,
        autoincrement=True
    )

    # SCRUM-6720: both are ATP ontologyterm ids; workflow_tag and topic are their curies
    workflow_tag_id = Column(BigInteger, ForeignKey("ontologyterm.id"), index=True, unique=True, nullable=False)
    workflow_tag = ontology_curie(workflow_tag_id)

    # Obtained from A-Team ontology node term-id
    topic_id = Column(BigInteger, ForeignKey("ontologyterm.id"), nullable=False)
    topic = ontology_curie(topic_id)


register_curie_columns(WorkflowTagTopicModel, {
    "workflow_tag": ("workflow_tag_id", ("ATPTerm",)),
    "topic": ("topic_id", ("ATPTerm",)),
})
