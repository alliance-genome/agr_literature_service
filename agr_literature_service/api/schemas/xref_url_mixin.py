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

`url` is chosen in three steps, most specific first:

1. The ENTITY PAGE. A person_cross_reference row is always about a person, so
   the descriptor's "person" page is asked for by name -- WB answers
   ``...;class=Person`` where default_url only gives the generic ``get?name=``.
   Subclasses name their kind in `entity_page_name`. This is the main path for
   person and laboratory records, whose `pages` column is NULL in practice.
2. An ABSOLUTE STORED PAGE. `pages` usually holds descriptor page names, but the
   SGD person loader writes the colleague's absolute obj_url there
   (lit_processing/oneoff_scripts/load_sgd_colleagues.py). That is the only real
   link those rows carry, and SGD's default_url has no ``[%s]`` at all, so step 3
   would hand every colleague the same homepage.
3. default_url, for a prefix whose descriptor defines no entity page.

Any explicit url already on the record wins over all three. `pages` entries come
out as ``{name, url}`` objects, matching ``CrossReferenceSchemaShow``, and are a
faithful copy of the column -- never synthesised from the entity-page lookup. The
write schemas keep taking plain strings, so no payload changes.
"""
from typing import Any, ClassVar, List, Optional

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from agr_literature_service.api.resource_descriptor_cache import (
    is_absolute_url,
    resolve_xref_urls,
)

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

        # Resolve the row's own pages and this record's entity page in one call:
        # the entity page is appended, so `resolved` stays aligned with
        # self.pages over its first len(stored_names) entries.
        lookup = list(stored_names or [])
        entity_index = None
        if self.entity_page_name:
            entity_index = len(lookup)
            lookup.append(self.entity_page_name)

        default_url, resolved = resolve_xref_urls(curie, lookup or None)

        if self.url is None:
            # 1. The entity page: this row's table already says what kind of
            #    record it is, so WB gives ...;class=Person rather than the
            #    generic get?name=. True whether or not the row stores pages.
            entity_url = None
            if entity_index is not None and resolved is not None:
                entity_url = resolved[entity_index].get("url")

            # 2. A stored page that is itself an absolute URL -- the SGD loader
            #    writes the colleague's obj_url there, and it is the only real
            #    link those rows have. SGD's default_url carries no [%s] at all,
            #    so substituting into it yields the bare homepage for everyone.
            #    Read from `name`: when the entry is absolute the name IS the
            #    link, and page.url is not populated until the loop below.
            absolute_url = next(
                (p.name for p in (self.pages or []) if is_absolute_url(p.name)),
                None,
            )

            # 3. default_url last.
            self.url = entity_url or absolute_url or default_url

        if self.pages is not None and stored_names and resolved is not None:
            for page, res in zip(self.pages, resolved):
                if page.url is None:
                    page.url = res["url"]
        return self
