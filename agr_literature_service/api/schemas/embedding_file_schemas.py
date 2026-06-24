from typing import Optional

from pydantic import BaseModel, ConfigDict


class EmbeddingFileSchemaBase(BaseModel):
    """Embedding catalog semantics (no vectors, no recipe descriptor)."""
    model_config = ConfigDict(extra='forbid', from_attributes=True)

    reference_curie: str
    profile_name: str
    version: int
    model_name: Optional[str] = None
    source_referencefile_id: Optional[int] = None


class EmbeddingFileSchemaCreate(EmbeddingFileSchemaBase):
    """Posted alongside the parquet file when registering an embedding.

    ``mod_abbreviation`` is forwarded to the parquet referencefile upload
    (referencefile_mod ownership); ``None`` = all-MOD (PMC).
    """
    mod_abbreviation: Optional[str] = None


class EmbeddingFileSchemaShow(EmbeddingFileSchemaBase):
    """Returned representation of a catalog row with its keys."""
    embedding_file_id: int
    parquet_referencefile_id: int
