"""
xref_url_mixin.py
=================
Shared mixin that turns a stored cross-reference curie into something the UI can
link to, using the A-team resource descriptors.

Reference cross references have always been resolved in the CRUD layer
(``cross_reference_crud.format_cross_reference_data``). Person and laboratory
cross references are served from several places -- nested inside the person or
laboratory record as well as from their own endpoints -- and their CRUD
functions return ORM objects, so resolving here covers every path with one
change instead of formatting at each call site.

`pages` is stored as a list of page NAMES; on the way out it becomes a list of
``{name, url}`` objects, matching ``CrossReferenceSchemaShow``. The write
schemas keep taking plain name strings, so no payload changes.
"""
from typing import Any, List, Optional

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from agr_literature_service.api.resource_descriptor_cache import resolve_xref_urls

from .cross_reference_schemas import CrossReferencePageSchemaShow


class ResolvedXrefUrlMixin(BaseModel):
    """Adds a resolved `url` and upgrades `pages` names into {name, url}."""

    model_config = ConfigDict(extra="forbid", from_attributes=True)

    url: Optional[str] = None
    pages: Optional[List[CrossReferencePageSchemaShow]] = None

    @field_validator("pages", mode="before")
    @classmethod
    def _accept_page_names(cls, v: Any) -> Any:
        """Accept the stored list of name strings, or already-resolved objects.

        Round-tripping an already-serialized record must not double-wrap, so
        anything that is not a bare string is passed through untouched.
        """
        if not isinstance(v, list):
            return v
        return [{"name": item, "url": None} if isinstance(item, str) else item for item in v]

    @model_validator(mode="after")
    def _resolve_urls(self) -> "ResolvedXrefUrlMixin":
        # An explicit url already on the record wins; otherwise derive it.
        # A page's name is Optional; substitute "" rather than dropping it, so
        # the list stays positionally aligned with self.pages for the zip below.
        # An empty name matches no descriptor page, so its url stays None.
        page_names = [p.name or "" for p in self.pages] if self.pages is not None else None
        default_url, resolved = resolve_xref_urls(getattr(self, "curie", "") or "", page_names)

        if self.url is None:
            self.url = default_url

        if self.pages is not None and resolved is not None:
            for page, res in zip(self.pages, resolved):
                if page.url is None:
                    page.url = res["url"]
        return self
