from typing import List, Dict, Optional

from pydantic import BaseModel


class FacetsOptionsSchema(BaseModel):
    query: Optional[str]
    facets_values: Optional[Dict[str, List[str]]]
    facets_limits: Optional[Dict[str, int]]
    return_facets_only: bool = False

