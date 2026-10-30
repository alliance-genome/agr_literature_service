"""
The Alliance category of a reference, from its MOD reference types, its
reference relations and its PubMed types.

At the start of the ABC each MOD translated its own reference types into an
Alliance category (the Lit WG "MOD Reference Types - Mapping" sheet) and sent
it in its DQM file, but the PubMed update then replaced the category with one
from the PubMed types alone, so a paper a MOD typed as a review became a
research article again whenever PubMed called it a "Journal Article". The
mapping now lives in data_for_alliance_category/ and every category comes
from all three sources:

- each MOD reference type maps to a category, or to none for a generic type
  such as "Journal Article" or a flag such as WB "Experimental";
- a RetractionOf / ErratumFor / CommentOn relation from the reference makes
  it a Retraction / Correction / Comment;
- the PubMed types map to a category (xml_to_json).

When these differ, the first in CATEGORY_PRIORITY wins: a review from any MOD
outranks PubMed's research article, a retraction outranks everything. A
reference with no category from any source keeps the one it has (e.g. a MOD's
own DQM allianceCategory for a paper with no PMID). The category of an
obsolete reference, or one a curator set by hand, is left alone.
"""
import csv
from dataclasses import dataclass, field
from functools import lru_cache
from os import path
from typing import Dict, Iterable, List, Optional, Set, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session

from agr_literature_service.api.models import ReferenceModel
from agr_literature_service.api.schemas.reference_category_schemas import ReferenceCategory

MAPPING_FILE = path.join(path.dirname(path.abspath(__file__)), "data_for_alliance_category",
                         "modReferenceType2allianceCategory_mapping.tsv")

# first wins; Research_Article is near the end so any more specific category
# from a MOD, a relation or PubMed outranks it
CATEGORY_PRIORITY = [
    "Retraction",
    "Correction",
    "Review_Article",
    "Comment",
    "Personal_Communication",
    "Thesis",
    "Book",
    "Conference_Publication",
    "Preprint",
    "Direct_Data_Submission",
    "Internal_Process_Reference",
    "Other",
    "Research_Article",
    "Unknown",
]
_RANK = {category: rank for rank, category in enumerate(CATEGORY_PRIORITY)}

RELATION_CATEGORY = {
    "RetractionOf": "Retraction",
    "ErratumFor": "Correction",
    "CommentOn": "Comment",
}

UNCHANGED_CATEGORIES = {"Obsolete"}


@dataclass
class CategoryInputs:
    """What a reference's category is computed from, besides its PubMed types."""
    mod_reference_types: List[Tuple[str, str]] = field(default_factory=list)
    relation_types: Set[str] = field(default_factory=set)
    set_by_curator: bool = False


@lru_cache(maxsize=1)
def load_mapping() -> Dict[Tuple[str, str], Optional[str]]:
    """(mod, lower-cased MOD reference type) -> category, or None for a type
    that gives no category."""
    mapping: Dict[Tuple[str, str], Optional[str]] = {}
    with open(MAPPING_FILE) as fh:
        rows = csv.DictReader((line for line in fh if not line.startswith("#")), delimiter="\t")
        for row in rows:
            mapping[(row["mod"], row["mod_reference_type"].lower())] = row["alliance_category"] or None
    return mapping


def category_name(category) -> Optional[str]:
    """'Review_Article' from a ReferenceCategory member, its name, its value
    ('review_article', as the API and DQM updates use) or its str()."""
    if category is None:
        return None
    if isinstance(category, ReferenceCategory):
        return category.name
    name = str(category).replace("ReferenceCategory.", "").strip().replace(" ", "_")
    for member in ReferenceCategory:
        if name.lower() == member.value:
            return member.name
    return name


def highest_priority(categories: Iterable[Optional[str]]) -> Optional[str]:
    known = [c for c in categories if c in _RANK]
    return min(known, key=_RANK.__getitem__) if known else None


def mod_reference_type_category(mod_reference_types: Iterable[Tuple[str, str]]) -> Optional[str]:
    """The category the reference's MOD reference types give, if any."""
    mapping = load_mapping()
    return highest_priority(mapping.get((mod, label.lower())) for mod, label in mod_reference_types)


def pubmed_types_category(pubmed_types: Optional[List[str]]) -> Optional[str]:
    if not pubmed_types:
        return None
    # imported here: xml_to_json pulls in the PubMed processing modules
    from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.xml_to_json import \
        get_alliance_category_from_pubmed_types
    return get_alliance_category_from_pubmed_types(pubmed_types)


def resolve_category(inputs: CategoryInputs, pubmed_category: Optional[str], current) -> Optional[str]:
    """The category the reference should have; its current one when nothing
    gives a category, or when it is obsolete or was set by a curator."""
    current = category_name(current)
    if current in UNCHANGED_CATEGORIES or inputs.set_by_curator:
        return current
    candidates = [mod_reference_type_category(inputs.mod_reference_types),
                  category_name(pubmed_category)]
    candidates += [RELATION_CATEGORY.get(relation_type) for relation_type in inputs.relation_types]
    return highest_priority(candidates) or current


def load_category_inputs(db: Session, reference_ids: Iterable[int]) -> Dict[int, CategoryInputs]:
    """CategoryInputs of each reference, three queries for the lot."""
    ids = sorted(set(reference_ids))
    inputs: Dict[int, CategoryInputs] = {reference_id: CategoryInputs() for reference_id in ids}
    if not ids:
        return inputs
    for reference_id, mod, label in db.execute(text(
            "SELECT rmrt.reference_id, m.abbreviation, rt.label "
            "FROM reference_mod_referencetype rmrt "
            "JOIN mod_referencetype mrt ON mrt.id = rmrt.mod_referencetype_id "
            "JOIN referencetype rt ON rt.id = mrt.referencetype_id "
            "JOIN mod m ON m.id = mrt.mod_id "
            "WHERE rmrt.reference_id = ANY(:ids)"), {"ids": ids}):
        inputs[reference_id].mod_reference_types.append((mod, label))
    for reference_id, relation_type in db.execute(text(
            "SELECT reference_id_from, reference_relation_type::text FROM reference_relation "
            "WHERE reference_id_from = ANY(:ids)"), {"ids": ids}):
        inputs[reference_id].relation_types.add(relation_type)
    # the latest change of the category was made by a person
    for (reference_id,) in db.execute(text(
            "SELECT DISTINCT ON (rv.id) rv.id FROM reference_version rv "
            "JOIN transaction t ON t.id = rv.transaction_id "
            "WHERE rv.id = ANY(:ids) AND rv.category_mod "
            "AND EXISTS (SELECT 1 FROM agent a WHERE a.id = t.user_id AND a.person_id IS NOT NULL) "
            "AND NOT EXISTS (SELECT 1 FROM reference_version later "
            "                WHERE later.id = rv.id AND later.category_mod "
            "                AND later.transaction_id > rv.transaction_id) "
            "ORDER BY rv.id, rv.transaction_id DESC"), {"ids": ids}):
        inputs[reference_id].set_by_curator = True
    return inputs


def set_reference_category(db: Session, reference_id: int) -> Optional[Tuple[Optional[str], str]]:
    """Set the reference's category from its MOD reference types, relations and
    stored PubMed types. Returns (old, new) when it changed. Flushes, does not
    commit."""
    reference = db.get(ReferenceModel, reference_id)
    if reference is None:
        return None
    db.flush()
    inputs = load_category_inputs(db, [reference_id])[reference_id]
    old = category_name(reference.category)
    new = resolve_category(inputs, pubmed_types_category(reference.pubmed_types), old)
    if new is None or new == old:
        return None
    reference.category = new
    db.flush()
    return old, new
