from typing import List, Dict, Optional

from pydantic import BaseModel


class FacetsOptionsSchema(BaseModel):
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
    tet_nested_facets_values: Optional[Dict] = {}
