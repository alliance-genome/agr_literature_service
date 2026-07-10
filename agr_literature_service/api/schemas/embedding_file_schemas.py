from typing import Optional

from pydantic import BaseModel, ConfigDict


class EmbeddingFileSchemaBase(BaseModel):
    """Embedding catalog semantics (no vectors, no recipe descriptor)."""
    # protected_namespaces=() so the `model_name` field doesn't trip Pydantic
    # v2's `model_` protected-namespace warning.
    model_config = ConfigDict(extra='forbid', from_attributes=True,
                              protected_namespaces=())

    reference_curie: str
    profile_name: str
    version: int
    model_name: Optional[str] = None
    source_referencefile_id: Optional[int] = None


class EmbeddingFileSchemaCreate(EmbeddingFileSchemaBase):
    """Payload for registering an embedding parquet (ABC-internal producers
    only — there is no public create endpoint).

    Access is NOT part of the payload: the parquet's referencefile_mod set is
    inherited from the source referencefile (MOD-specific source -> same MODs;
    open/PMC source -> open), so a producer cannot register a restricted
    paper's embedding under broader access than its source.
    """


class EmbeddingFileSchemaShow(EmbeddingFileSchemaBase):
    """Returned representation of a catalog row with its keys."""
    embedding_file_id: int
    parquet_referencefile_id: int
