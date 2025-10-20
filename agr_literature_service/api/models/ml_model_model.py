from sqlalchemy import Column, Integer, String, Float, ForeignKey, UniqueConstraint, Boolean
from sqlalchemy.orm import relationship, mapped_column, Mapped
from agr_literature_service.api.database.base import Base


class MLModel(Base):
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

    novel_topic_qualifier = Column(
        String,
        nullable=True
    )

    __table_args__ = (
        UniqueConstraint('task_type', 'mod_id', 'topic', 'version_num', name='uq_ml_model_task_mod_topic_version'),
    )
