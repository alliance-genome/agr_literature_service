import logging
from dataclasses import dataclass, field
from typing import List

from fastapi import HTTPException, status
from sqlalchemy import or_
from sqlalchemy.orm import Session, Load

from agr_literature_service.api.models import ReferenceModel, ObsoleteReferenceModel, ReferencefileModel

logger = logging.getLogger(__name__)


class Citation:
    def __init__(self, volume="", pages=""):
        self._volume = ""
        self.volume = volume
        self._pages = ""
        self.pages = pages

    @property
    def volume(self):
        return self._volume

    @volume.setter
    def volume(self, vol_value):
        if vol_value:
            self._volume = vol_value

    @property
    def pages(self):
        return self._pages

    @pages.setter
    def pages(self, pages_value):
        if pages_value:
            self._pages = pages_value


@dataclass
class BibInfo:
    _authors: List[str] = field(default_factory=lambda: [])
    cross_references: List[str] = field(default_factory=lambda: [])
    pubmed_types: List[str] = field(default_factory=lambda: [])
    title: str = ""
    journal: str = ""
    _citation: str = ""
    year: str = ""
    abstract: str = ""
    reference_curie: str = ""

    @property
    def citation(self):
        return self._citation

    @citation.setter
    def citation(self, citation: Citation):
        self._citation = f"V: {citation.volume}P: {citation.pages}"

    @property
    def authors(self) -> List[str]:
        return self._authors

    def add_author(self, last_name: str, first_initial: str, name: str):
        if last_name and first_initial:
            self._authors.append(f"{last_name} {first_initial}")
        elif name:
            self._authors.append(name)

    def get_formatted_bib(self, format_type: str = 'txt'):
        if format_type == 'txt':
            return f"author|{' ; '.join(self.authors)}\n" \
                   f"accession|{' '.join(self.cross_references)} {self.reference_curie}\n" \
                   f"type|{' ; '.join(self.pubmed_types)}\n" \
                   f"title|{self.title}\n" \
                   f"journal|{self.journal}\n" \
                   f"citation|{self.citation}\n" \
                   f"year|{self.year}\n" \
                   f"abstract|{self.abstract}\n"


def get_merged(db: Session, curie, query_options=None):
    logger.debug("Looking up if '{}' is a merged entry".format(curie))
    # Is the curie in the merged set
    try:
        obs_ref_cur: ObsoleteReferenceModel = db.query(ObsoleteReferenceModel).filter(
            ObsoleteReferenceModel.curie == curie).one_or_none()
    except Exception:
        logger.debug("No merge data found so give error message")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference with the id {curie} is not available")

    # If found in merge then get new reference.
    if obs_ref_cur:
        logger.debug("Merge found looking up the id '{}' instead now".format(obs_ref_cur.new_id))
    try:
        query = db.query(ReferenceModel)
        if query_options:
            query = query.options(query_options)
        reference = query.filter(ReferenceModel.reference_id == obs_ref_cur.new_id).one_or_none()
    except Exception:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference with the id {curie} is not available")
    return reference


def get_reference(db: Session, curie_or_reference_id: str, load_referencefiles: bool = False,
                  load_authors: bool = False, load_mod_corpus_associations: bool = False,
                  load_mesh_terms: bool = False, load_obsolete_references: bool = False):
    reference_id = int(curie_or_reference_id) if curie_or_reference_id.isdigit() else None
    options = None
    query = db.query(ReferenceModel)
    if load_referencefiles or load_authors or load_mod_corpus_associations or load_mesh_terms or \
            load_obsolete_references:
        options = Load(ReferenceModel)
        if load_referencefiles:
            options.subqueryload(ReferenceModel.referencefiles).subqueryload(
                ReferencefileModel.referencefile_mods)
        if load_authors:
            options.joinedload(ReferenceModel.author)
        if load_mod_corpus_associations:
            options.joinedload(ReferenceModel.mod_corpus_association)
        if load_mesh_terms:
            options.joinedload(ReferenceModel.mesh_term)
        if load_obsolete_references:
            options.joinedload(ReferenceModel.obsolete_reference)
        query = query.options(options)
    reference = query.filter(or_(ReferenceModel.curie == curie_or_reference_id,
                                 ReferenceModel.reference_id == reference_id)).one_or_none()
    if reference is None and reference_id is None:
        reference = get_merged(db, curie_or_reference_id, options)
        logger.debug("Found from merged '{}'".format(reference))
    if not reference:
        logger.warning("Reference not found for {}?".format(curie_or_reference_id))
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference with the reference_id or curie {curie_or_reference_id} is not available")
    return reference
