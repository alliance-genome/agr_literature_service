"""Merge papers that are in ABC twice: once with a DOI and once with its PMID (SCRUM-6864).

Until SCRUM-6864, the PubMed search skipped the duplicate check for the PMID
after each PMID it attached to a DOI record, and a DQM load could create a
reference for a new PMID whose DOI had matched an existing one. Either way the
paper ended up as two references: one from a MOD DQM load with the DOI and the
MOD's ID, and one from PubMed with the PMID (and PMCID) but no DOI, since DOIs
are unique. pubmed_update_references_by_doi finds these pairs every week
("Papers with DOI Resolving to the Same PMID") but only reports them.

This script finds the same pairs the same way and merges each one into the DOI
record, which holds most of the curation (MOD files, workflow tags, tags):

- cross references of the PMID record (PMID, PMCID, other MODs' IDs) move to
  the DOI record; its DOI, if any, is a case variant and is dropped;
- MOD corpus associations, MOD reference types, workflow tags, curation
  status, indexing priority, emails and embedding files move unless the DOI
  record already has them for that MOD (a null corpus takes the other value);
- files move, and a file with the same md5sum is merged into the DOI record's;
- reference_crud.merge_references then moves tags and relations, records the
  PMID record's curie as obsolete, deletes it and refreshes the DOI record
  from PubMed (title, authors, ...).

A pair is left for a curator (Merge page in the UI) when the two records
disagree: different IDs from the same MOD (the MOD has the paper twice),
corpus true on one and false on the other, or workflow tags, curation status
or indexing priority for the same MOD on both.

Usage:
    python SCRUM-6864_merge_doi_pmid_duplicates.py                  # dry run: report only
    python SCRUM-6864_merge_doi_pmid_duplicates.py --apply
    python SCRUM-6864_merge_doi_pmid_duplicates.py --pairs pairs.tsv  # DOI-record curie <TAB> PMID, skips PubMed
"""
import argparse
import csv
import logging
import time
from typing import Dict, List, NamedTuple, Optional, Set, Tuple

from sqlalchemy.orm import Session

from agr_literature_service.api.crud import reference_crud, referencefile_crud
from agr_literature_service.api.models import CrossReferenceModel, CurationStatusModel, EmbeddingFileModel, \
    IndexingPriorityModel, ModCorpusAssociationModel, ModModel, ReferenceEmailModel, ReferencefileModel, \
    ReferenceModel, ReferenceModReferencetypeAssociationModel, WorkflowTagModel
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.pubmed_update_references_by_doi import \
    classify_doi_matches, get_cross_reference_data, get_dois_for_pmids, get_pmid_for_doi
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

# basicConfig, not fileConfig: fileConfig disables every logger already
# created, which breaks log-capturing tests that import the script
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SCRIPT_NAME = "SCRUM-6864_merge_doi_pmid_duplicates"


class Side(NamedTuple):
    """What one reference of a pair holds that decides how the pair merges."""
    curie: str
    xrefs: Dict[str, str]              # prefix -> curie, valid cross references only
    corpus: Dict[str, Optional[bool]]  # MOD abbreviation -> mod_corpus_association.corpus
    workflow_mods: Set[str]
    curation_status_mods: Set[str]
    indexing_priority_mods: Set[str]


def find_conflicts(doi_side: Side, pmid_side: Side) -> List[str]:
    """Reasons a pair needs a curator; empty when it can be merged automatically."""
    conflicts = []
    for prefix, curie in pmid_side.xrefs.items():
        if prefix not in ('DOI', 'PMID', 'PMCID') and prefix in doi_side.xrefs and doi_side.xrefs[prefix] != curie:
            conflicts.append(f"two {prefix} IDs: {doi_side.xrefs[prefix]} and {curie}")
    for prefix in ('PMID', 'PMCID'):
        if prefix in doi_side.xrefs and prefix in pmid_side.xrefs and doi_side.xrefs[prefix] != pmid_side.xrefs[prefix]:
            conflicts.append(f"two {prefix}s: {doi_side.xrefs[prefix]} and {pmid_side.xrefs[prefix]}")
    for mod_id, corpus in pmid_side.corpus.items():
        if mod_id in doi_side.corpus and None not in (corpus, doi_side.corpus[mod_id]) \
                and corpus != doi_side.corpus[mod_id]:
            conflicts.append(f"{mod_id} corpus {doi_side.corpus[mod_id]} and {corpus}")
    for label, doi_mods, pmid_mods in (
            ("workflow tags", doi_side.workflow_mods, pmid_side.workflow_mods),
            ("curation status", doi_side.curation_status_mods, pmid_side.curation_status_mods),
            ("indexing priority", doi_side.indexing_priority_mods, pmid_side.indexing_priority_mods)):
        for mod_id in sorted(doi_mods & pmid_mods):
            conflicts.append(f"{mod_id} {label} on both")
    return conflicts


def load_side(db: Session, reference: ReferenceModel) -> Side:
    rid = reference.reference_id
    abbreviation = {m.mod_id: m.abbreviation for m in db.query(ModModel).all()}

    def mods(model) -> Set[str]:
        return {abbreviation[row.mod_id] for row in db.query(model.mod_id).filter(model.reference_id == rid).all()}

    xrefs = {x.curie_prefix: x.curie for x in db.query(CrossReferenceModel).filter(
        CrossReferenceModel.reference_id == rid, CrossReferenceModel.is_obsolete.is_(False)).all()}
    corpus = {abbreviation[m.mod_id]: m.corpus for m in db.query(ModCorpusAssociationModel).filter(
        ModCorpusAssociationModel.reference_id == rid).all()}
    return Side(reference.curie, xrefs, corpus, mods(WorkflowTagModel), mods(CurationStatusModel),
                mods(IndexingPriorityModel))


def find_pairs_from_pubmed(db: Session) -> List[Tuple[str, str]]:
    """(DOI-record curie, "PMID:n") for every DOI-only reference whose DOI resolves to a PMID on another
    reference, found the way pubmed_update_references_by_doi finds them."""
    (reference_id_to_pmid, pmid_to_reference_id, reference_id_to_doi) = get_cross_reference_data(db)
    candidates = []
    doi_only = [rid for rid in reference_id_to_doi if rid not in reference_id_to_pmid]
    logger.info(f"Looking up {len(doi_only)} DOI-only references in PubMed")
    for n, reference_id in enumerate(doi_only, 1):
        doi = reference_id_to_doi[reference_id]
        pmids = get_pmid_for_doi(doi)
        if pmids and len(pmids) == 1:
            candidates.append((reference_id, doi, pmids[0]))
        if n % 200 == 0:
            logger.info(f"  {n} of {len(doi_only)} looked up")
        time.sleep(0.35)
    pmid_to_dois = get_dois_for_pmids([pmid for (_, _, pmid) in candidates])
    papers_to_merge = classify_doi_matches(candidates, pmid_to_dois, pmid_to_reference_id)[1]
    doi_to_reference_id = {doi: rid for (rid, doi, _) in candidates}
    pairs = []
    for (doi, pmid) in papers_to_merge:
        reference = db.query(ReferenceModel).filter(ReferenceModel.reference_id == doi_to_reference_id[doi]).one()
        pairs.append((reference.curie, pmid))
    return pairs


def read_pairs(path: str) -> List[Tuple[str, str]]:
    with open(path) as fh:
        return [(row[0], row[1] if row[1].startswith("PMID:") else "PMID:" + row[1])
                for row in csv.reader(fh, delimiter="\t") if row]


def move_rows(db: Session, model, from_id: int, to_id: int):
    for row in db.query(model).filter(model.reference_id == from_id).all():
        row.reference_id = to_id


def merge_pair(db: Session, doi_ref: ReferenceModel, pmid_ref: ReferenceModel, doi_side: Side):
    """Move what the PMID record holds onto the DOI record, then merge the PMID record into it."""
    w, l_id = doi_ref.reference_id, pmid_ref.reference_id

    for x in db.query(CrossReferenceModel).filter(CrossReferenceModel.reference_id == l_id).all():
        if x.curie_prefix == 'DOI' or x.curie in doi_side.xrefs.values():
            db.delete(x)
        else:
            x.reference_id = w

    doi_mcas = {m.mod_id: m for m in db.query(ModCorpusAssociationModel).filter(
        ModCorpusAssociationModel.reference_id == w).all()}
    for mca in db.query(ModCorpusAssociationModel).filter(ModCorpusAssociationModel.reference_id == l_id).all():
        kept = doi_mcas.get(mca.mod_id)
        if kept is None:
            mca.reference_id = w
            continue
        if kept.corpus is None and mca.corpus is not None:
            kept.corpus = mca.corpus
            kept.mod_corpus_sort_source = mca.mod_corpus_sort_source
        db.delete(mca)

    doi_types = {t.mod_referencetype_id for t in db.query(ReferenceModReferencetypeAssociationModel).filter(
        ReferenceModReferencetypeAssociationModel.reference_id == w).all()}
    for t in db.query(ReferenceModReferencetypeAssociationModel).filter(
            ReferenceModReferencetypeAssociationModel.reference_id == l_id).all():
        if t.mod_referencetype_id in doi_types:
            db.delete(t)  # the FK does not cascade, so a duplicate would block deleting the PMID record
        else:
            t.reference_id = w

    # find_conflicts holds back pairs with these on both sides for a MOD, so all of them can move
    for model in (WorkflowTagModel, CurationStatusModel, IndexingPriorityModel):
        move_rows(db, model, l_id, w)
    doi_emails = {e.email_address for e in db.query(ReferenceEmailModel).filter(
        ReferenceEmailModel.reference_id == w).all()}
    for e in db.query(ReferenceEmailModel).filter(ReferenceEmailModel.reference_id == l_id).all():
        if e.email_address not in doi_emails:
            e.reference_id = w
    db.commit()

    doi_files = {f.md5sum: f.referencefile_id for f in db.query(ReferencefileModel).filter(
        ReferencefileModel.reference_id == w).all()}
    for f in db.query(ReferencefileModel).filter(ReferencefileModel.reference_id == l_id).all():
        if f.md5sum in doi_files:
            referencefile_crud.merge_referencefiles(db, doi_ref.curie, f.referencefile_id, doi_files[f.md5sum])
        else:
            referencefile_crud.patch(db, f.referencefile_id, {"reference_curie": doi_ref.curie})
    move_rows(db, EmbeddingFileModel, l_id, w)
    db.commit()

    reference_crud.merge_references(db, pmid_ref.curie, doi_ref.curie)


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--apply", action="store_true", help="merge the pairs; without it only report")
    parser.add_argument("--pairs", help="TSV of DOI-record curie and PMID, instead of looking them up in PubMed")
    args = parser.parse_args()

    db = create_postgres_session(False)
    set_global_user_id(db, SCRIPT_NAME)
    pairs = read_pairs(args.pairs) if args.pairs else find_pairs_from_pubmed(db)
    logger.info(f"{len(pairs)} DOI/PMID duplicate pairs")

    # merge_references reports each merge; one summary at the end is enough for a batch
    reference_crud.send_report = lambda subject, message: logger.info(f"{subject}: {message}")

    merged, held, failed = [], [], []
    for doi_curie, pmid in pairs:
        doi_ref = db.query(ReferenceModel).filter(ReferenceModel.curie == doi_curie).one_or_none()
        pmid_xref = db.query(CrossReferenceModel).filter(
            CrossReferenceModel.curie == pmid, CrossReferenceModel.is_obsolete.is_(False),
            CrossReferenceModel.reference_id.isnot(None)).one_or_none()
        if doi_ref is None or pmid_xref is None or pmid_xref.reference_id == doi_ref.reference_id:
            logger.info(f"SKIP {doi_curie} {pmid}: already merged or gone")
            continue
        pmid_ref = db.query(ReferenceModel).filter(ReferenceModel.reference_id == pmid_xref.reference_id).one()
        doi_side, pmid_side = load_side(db, doi_ref), load_side(db, pmid_ref)
        conflicts = find_conflicts(doi_side, pmid_side)
        if conflicts:
            held.append((doi_curie, pmid_ref.curie, pmid, "; ".join(conflicts)))
            logger.info(f"HOLD {doi_curie} <- {pmid_ref.curie} ({pmid}): {'; '.join(conflicts)}")
            continue
        if not args.apply:
            merged.append((doi_curie, pmid_ref.curie, pmid))
            logger.info(f"WOULD MERGE {pmid_ref.curie} ({pmid}) into {doi_curie}")
            continue
        try:
            merge_pair(db, doi_ref, pmid_ref, doi_side)
            merged.append((doi_curie, pmid_ref.curie, pmid))
            logger.info(f"MERGED {pmid_ref.curie} ({pmid}) into {doi_curie}")
        except Exception as e:
            db.rollback()
            failed.append((doi_curie, pmid_ref.curie, pmid, str(e)))
            logger.error(f"FAILED {pmid_ref.curie} ({pmid}) into {doi_curie}: {e}")

    verb = "merged" if args.apply else "would merge"
    logger.info(f"SUMMARY: {verb} {len(merged)}, held for curators {len(held)}, failed {len(failed)}")
    for row in held:
        logger.info("HELD\t" + "\t".join(row))
    for row in failed:
        logger.info("FAILED\t" + "\t".join(row))
    db.close()


if __name__ == "__main__":
    main()
