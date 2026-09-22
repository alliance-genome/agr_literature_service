from sqlalchemy import Column, Integer, String, Float, ForeignKey, UniqueConstraint, Boolean, ARRAY
from sqlalchemy.orm import relationship, mapped_column, Mapped
from agr_literature_service.api.database.base import Base
from agr_literature_service.api.models.audited_model import AuditedModel


class MLModel(AuditedModel, Base):
    __tablename__ = 'ml_model'

    ml_model_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    task_type = Column(
        String,
        index=True
    )

    model_type = Column(
        String,
        index=True
    )

    file_extension = Column(String)

    mod_id = Column(
        Integer,
        ForeignKey("mod.mod_id", ondelete="CASCADE"),
        index=True,
        nullable=False
    )

    mod = relationship(
        "ModModel",
        foreign_keys="MLModel.mod_id"
    )

    topic = Column(
        String,
        index=True
    )

    version_num = Column(
        Integer,
        index=True,
        nullable=False
    )

    precision = Column(Float)

    recall = Column(Float)

    f1_score = Column(Float)

    parameters = Column(String)

    dataset_id = Column(
        Integer,
        ForeignKey('dataset.dataset_id', ondelete="CASCADE"),
        index=True,
        nullable=True
    )

    dataset = relationship(
        "DatasetModel",
        foreign_keys="MLModel.dataset_id"
    )

    species = Column(
        String(),
        unique=False,
        nullable=True
    )

    production: Mapped[bool] = mapped_column(
        Boolean,
        nullable=True,
        default=False
    )

    negated: Mapped[bool] = mapped_column(
        Boolean,
        nullable=True,
        default=True
    )

    data_novelty = Column(
        String,
        nullable=True
    )

    # SCRUM-5697. The data_context term the pipelines stamp onto every tag this
    # model creates, so the curation policy lives on the model row rather than
    # in pipeline code (same arrangement as data_novelty above).
    data_context = Column(
        String,
        nullable=True
    )

    file_classes = Column(
        ARRAY(String),
        nullable=True
    )

    description = Column(
        String,
        nullable=True
    )

    # ABC-embedding recipe (SCRUM-5781). NULL for legacy BioWordVec models. When
    # embedding_profile is set, the model was trained on the ABC's precomputed
    # embeddings: (profile, version) is what the classifier needs to select which
    # stored embedding to fetch for a reference. Everything else (model, dim,
    # pooling, BoW) is a fixed convention read from the parquet or hard-coded.
    embedding_profile = Column(String, index=True, nullable=True)

    embedding_version = Column(Integer, nullable=True)

    __table_args__ = (
        UniqueConstraint('task_type', 'mod_id', 'topic', 'version_num', name='uq_ml_model_task_mod_topic_version'),
    )
