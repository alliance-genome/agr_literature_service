from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class FacetsOptionsSchema(BaseModel):
    """Schema for search facets, pagination, and filtering options."""
    model_config = ConfigDict(
        extra='forbid',       # forbid unexpected fields
        from_attributes=True  # allow ORM-like init if needed
    )

    query: Optional[str] = None
    facets_values: Optional[Dict[str, List[str]]] = None
    negated_facets_values: Optional[Dict[str, List[str]]] = None
    facets_limits: Optional[Dict[str, int]] = None
    size_result_count: Optional[int] = None
    sort_by_published_date_order: Optional[str] = None
    page: Optional[int] = None
    return_facets_only: bool = False
    author_filter: Optional[str] = None
    date_pubmed_modified: Optional[List[str]] = None
    date_pubmed_arrive: Optional[List[str]] = None
    date_published: Optional[List[str]] = None
    date_created: Optional[List[str]] = None
    query_fields: Optional[str] = None
    partial_match: bool = True
    tet_nested_facets_values: Dict[str, Any] = Field(default_factory=dict)

    # the tests send "query_field" (singular) and "sort"
    sort: Optional[List[Dict[str, Any]]] = None
    query_field: Optional[str] = None
