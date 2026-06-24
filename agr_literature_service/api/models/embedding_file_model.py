from sqlalchemy import Column, Integer, String, ForeignKey, Index
from sqlalchemy.orm import relationship

from agr_literature_service.api.database.base import Base


class EmbeddingFileModel(Base):
    """Lean, non-audited catalog of reference embedding parquet files.

    One row per (reference, profile_name, version, source_referencefile_id);
    that tuple maps 1:1 to a single stored parquet (an ``embedding``
    referencefile). NO vectors, recipe descriptor, or md5 here -- those live
    in the parquet file metadata and on the parquet referencefile row, which
    is the audited/versioned artifact. See SCRUM-6141.
    """
    __tablename__ = "embedding_file"

    embedding_file_id = Column(Integer, primary_key=True, autoincrement=True)

    reference_id = Column(
        Integer,
        ForeignKey("reference.reference_id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )
    reference = relationship(
        "ReferenceModel", foreign_keys="EmbeddingFileModel.reference_id"
    )

    profile_name = Column(String, index=True, nullable=False)
    version = Column(Integer, nullable=False)
    model_name = Column(String, index=True, nullable=True)

    # The converted_merged_* markdown that was embedded; NULL for abstracts.
    source_referencefile_id = Column(
        Integer,
        ForeignKey("referencefile.referencefile_id", ondelete="CASCADE"),
        index=True,
        nullable=True,
    )
    source_referencefile = relationship(
        "ReferencefileModel",
        foreign_keys="EmbeddingFileModel.source_referencefile_id",
    )

    # The stored embedding parquet (storage + download delegated to it).
    parquet_referencefile_id = Column(
        Integer,
        ForeignKey("referencefile.referencefile_id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )
    parquet_referencefile = relationship(
        "ReferencefileModel",
        foreign_keys="EmbeddingFileModel.parquet_referencefile_id",
    )

    # PG13 has no NULLS NOT DISTINCT, so enforce the unique key with two
    # partial indexes (the referencefile_mod pattern).
    __table_args__ = (
        Index(
            "uq_embedding_file_with_source",
            "reference_id", "profile_name", "version", "source_referencefile_id",
            unique=True,
            postgresql_where=(source_referencefile_id.isnot(None)),
        ),
        Index(
            "uq_embedding_file_abstract",
            "reference_id", "profile_name", "version",
            unique=True,
            postgresql_where=(source_referencefile_id.is_(None)),
        ),
    )
