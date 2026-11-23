"""Every paper in the SGD corpus has an active SGD curie (SCRUM-6830).

A paper reaches the SGD corpus through the API, the PubMed and DQM loaders,
the GAF and interaction loaders, and topic entity tags. Each path used to mint
the SGD curie itself, or not, so papers ended up in the corpus without one. A
before_flush listener now does it for all of them: when a SGD
mod_corpus_association is saved with corpus True, the paper gets an active SGD
curie in the same flush.

A paper moved out of the corpus keeps its SGD curie as obsolete. When it comes
back in, that curie is made active again, so the paper keeps the SGD ID SGD
already knows it by. A new one is minted from sgd_id_seq only when the paper
never had one, or when another paper now holds the old one (an active curie is
unique, idx_curie).
"""
from typing import List, Optional

from sqlalchemy import event, inspect as sa_inspect
from sqlalchemy.orm import Session

from agr_literature_service.api.models.cross_reference_model import CrossReferenceModel, sgd_id_seq
from agr_literature_service.api.models.mod_corpus_association_model import ModCorpusAssociationModel
from agr_literature_service.api.models.mod_model import ModModel


def _sgd_xrefs(session, reference_id, reference) -> List[CrossReferenceModel]:
    """The paper's SGD cross references, the saved ones and the ones still
    pending in this session, newest first."""
    saved: List[CrossReferenceModel] = []
    if reference_id is not None:
        saved = session.query(CrossReferenceModel).filter(
            CrossReferenceModel.reference_id == reference_id,
            CrossReferenceModel.curie_prefix == 'SGD').order_by(
            CrossReferenceModel.cross_reference_id.desc()).all()
    pending = [obj for obj in session.new
               if isinstance(obj, CrossReferenceModel)
               and (obj.curie or '').startswith('SGD:')
               and ((reference_id is not None and obj.reference_id == reference_id)
                    or (reference is not None and obj.reference is reference))]
    return [x for x in pending + saved if x not in session.deleted]


def _curie_is_active_elsewhere(session, xref) -> bool:
    if any(isinstance(obj, CrossReferenceModel) and obj is not xref and obj.curie == xref.curie
           and not obj.is_obsolete for obj in session.new):
        return True
    query = session.query(CrossReferenceModel.cross_reference_id).filter(
        CrossReferenceModel.curie == xref.curie,
        CrossReferenceModel.is_obsolete.is_(False))
    if xref.cross_reference_id is not None:
        query = query.filter(CrossReferenceModel.cross_reference_id != xref.cross_reference_id)
    return query.first() is not None


def ensure_active_sgd_curie(session, reference_id: Optional[int] = None, reference=None) -> bool:
    """Give the paper an active SGD curie unless it has one: reactivate its
    latest obsolete one if no other paper holds it, else mint a new one.
    Returns True when a curie was reactivated or added."""
    with session.no_autoflush:
        xrefs = _sgd_xrefs(session, reference_id, reference)
        if any(not x.is_obsolete for x in xrefs):
            return False
        if xrefs and not _curie_is_active_elsewhere(session, xrefs[0]):
            xrefs[0].is_obsolete = False
            return True
        number = session.connection().execute(sgd_id_seq.next_value()).scalar()
        new_xref = CrossReferenceModel(curie=f"SGD:S{number}", curie_prefix='SGD',
                                       pages=['reference'], is_obsolete=False)
        if reference_id is not None:
            new_xref.reference_id = reference_id
        else:
            new_xref.reference = reference
        session.add(new_xref)
        return True


def _moved_into_corpus(mca) -> bool:
    if mca.corpus is not True:
        return False
    state = sa_inspect(mca)
    return state.pending or state.attrs.corpus.history.has_changes()


@event.listens_for(Session, "before_flush")
def _sgd_corpus_papers_get_sgd_curie(session, _flush_context, _instances):
    moved = [obj for obj in list(session.new) + list(session.dirty)
             if isinstance(obj, ModCorpusAssociationModel) and _moved_into_corpus(obj)]
    if not moved:
        return
    with session.no_autoflush:
        sgd_mod_id = session.query(ModModel.mod_id).filter(ModModel.abbreviation == 'SGD').scalar()
        for mca in moved:
            is_sgd = (mca.mod_id == sgd_mod_id) if mca.mod_id is not None \
                else (mca.mod is not None and mca.mod.abbreviation == 'SGD')
            if is_sgd:
                # An association can carry only its reference object (the crud
                # sets mca.reference), whose id is not copied to reference_id
                # until the flush. Use the saved paper's id then, or its saved
                # SGD curie is missed and a second one minted, which fails
                # idx_curie_prefix_ref_no_cgc. The relationship only for a
                # paper that is not saved yet.
                reference_id = mca.reference_id
                if reference_id is None and mca.reference is not None:
                    reference_id = mca.reference.reference_id
                ensure_active_sgd_curie(session, reference_id,
                                        mca.reference if reference_id is None else None)
