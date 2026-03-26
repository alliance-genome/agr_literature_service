from typing import Optional

from pydantic import BaseModel, ConfigDict


class ExternalLookupResponse(BaseModel):
    """Response schema for external lookup of references by curie."""
    model_config = ConfigDict(extra='forbid')

    exists_in_db: bool
    reference_curie: Optional[str] = None
    external_curie: str
    external_curie_found: bool
    title: Optional[str] = None
