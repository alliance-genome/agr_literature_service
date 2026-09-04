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

`pages` is stored as a list of strings that are USUALLY descriptor page names,
resolved against the descriptor's templates -- but not always: the SGD person
loader writes the colleague's absolute obj_url into that column
(lit_processing/oneoff_scripts/load_sgd_colleagues.py), and for those rows it is
the only link they carry. resolve_xref_urls handles both, treating an
already-absolute entry as its own url. Either way the entry comes out as a
``{name, url}`` object, matching ``CrossReferenceSchemaShow``. The write schemas
keep taking plain strings, so no payload changes.
"""
from typing import Any, ClassVar, List, Optional

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from agr_literature_service.api.resource_descriptor_cache import resolve_xref_urls

from .cross_reference_schemas import CrossReferencePageSchemaShow


class ResolvedXrefUrlMixin(BaseModel):
    """Adds a resolved `url` and upgrades `pages` names into {name, url}."""

    model_config = ConfigDict(extra="forbid", from_attributes=True)

    # The descriptor page that describes this kind of record, e.g. "person".
    # Subclasses set it; a person_cross_reference row is always about a person,
    # so the page name is fixed by the table rather than stored per row (unlike
    # reference xrefs, where `pages` genuinely varies row to row). ClassVar, so
    # pydantic does not treat it as a field.
    entity_page_name: ClassVar[Optional[str]] = None

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
        curie = getattr(self, "curie", "") or ""

        # A page's name is Optional; substitute "" rather than dropping it, so
        # the list stays positionally aligned with self.pages for the zip below.
        # An empty name matches no descriptor page, so its url stays None.
        stored_names = (
            [p.name or "" for p in self.pages] if self.pages is not None else None
        )

        # With no pages of its own, ask the descriptor for the page describing
        # this kind of record -- WB's "person" page gives
        # ...;class=Person where default_url only gives the generic get?name=.
        # The row stays untouched: `pages` is a faithful copy of the column and
        # is not synthesised from this lookup.
        lookup_names = stored_names
        if not stored_names and self.entity_page_name:
            lookup_names = [self.entity_page_name]

        default_url, resolved = resolve_xref_urls(curie, lookup_names)

        if self.url is None:
            entity_url = (
                resolved[0]["url"]
                if (not stored_names and resolved and resolved[0].get("url"))
                else None
            )
            # The entity page is the more specific link; default_url is the
            # fallback when the descriptor does not define one (e.g. SGD).
            self.url = entity_url or default_url

        if self.pages is not None and stored_names and resolved is not None:
            for page, res in zip(self.pages, resolved):
                if page.url is None:
                    page.url = res["url"]
        return self
