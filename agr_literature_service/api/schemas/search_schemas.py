from typing import List, Dict, Optional

from pydantic import BaseModel


class FacetsOptionsSchema(BaseModel):
    query: Optional[str]
    facets_values: Optional[Dict[str, List[str]]]
    negated_facets_values: Optional[Dict[str, List[str]]]
    facets_limits: Optional[Dict[str, int]]
    size_result_count: Optional[int]
    sort_by_published_date_order: Optional[str]
    page: Optional[int]
    return_facets_only: bool = False
    author_filter: Optional[str]
    date_pubmed_modified: Optional[List[str]] = None
    date_pubmed_arrive: Optional[List[str]] = None
    date_published: Optional[List[str]] = None
    date_created: Optional[List[str]] = None
    query_fields: Optional[str]
    partial_match: bool = True
    tet_nested_facets_values: Optional[Dict] = {}
