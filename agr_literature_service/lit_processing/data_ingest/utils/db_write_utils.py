from os import environ, makedirs, path
import json
from typing import List, Dict, Set, Tuple, Iterable, Optional

from sqlalchemy import or_, and_, text
from sqlalchemy.engine import Result
from sqlalchemy.orm import Session

from agr_literature_service.api.crud.mod_reference_type_crud import insert_mod_reference_type_into_db
from agr_literature_service.lit_processing.data_ingest.utils.author import Author, authors_lists_are_equal, \
    authors_have_same_name
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import \
    create_postgres_session
from agr_literature_service.lit_processing.utils.db_read_utils import \
    get_reference_id_by_curie, get_reference_id_by_pmid
from agr_literature_service.api.models import ReferenceModel, AuthorModel, \
    CrossReferenceModel, ModCorpusAssociationModel, ModModel, ReferenceRelationModel, \
    MeshDetailModel, ReferenceModReferencetypeAssociationModel, \
    ReferencefileModel, ReferencefileModAssociationModel, WorkflowTagModel
from agr_literature_service.api.crud.utils.patterns_check import check_pattern  # type: ignore
from agr_literature_service.api.crud.workflow_tag_crud import get_workflow_tags_from_process, \
    transition_to_workflow_status, get_current_workflow_status
from agr_literature_service.api.crud.reference_utils import get_reference

batch_size_for_commit = 250
file_upload_process_atp_id = "ATP:0000140"  # file upload
file_needed_tag_atp_id = "ATP:0000141"  # file needed
file_uploaded_tag_atp_id = "ATP:0000134"  # file needed


def add_file_uploaded_workflow(db_session: Session, curie_or_reference_id, mod=None, transition_type="automated", logger=None):  # pragma: no cover
    logger.info("Add file uploaded workflow")
    ref = get_reference(db=db_session, curie_or_reference_id=str(curie_or_reference_id))
    if ref is None:
        if logger:
            logger.info(f"Invalid curie_or_reference_id = {curie_or_reference_id}")
            return

    if mod is None:
        rows = db_session.execute(text(f"SELECT m.abbreviation "
                                       f"FROM mod m, mod_corpus_association mca "
                                       f"WHERE m.mod_id = mca.mod_id "
                                       f"AND mca.reference_id = {ref.reference_id} "
                                       f"AND mca.corpus is True")).mappings().fetchall()
        mods = {x['abbreviation'] for x in rows}
    else:
        mods = {mod}

    for mod in mods:
        try:
            curr_tag_atp_id = get_current_workflow_status(db_session, str(ref.reference_id),
                                                          file_upload_process_atp_id, mod)
            logger.info(f"curr_tag_atp_id={curr_tag_atp_id}")
            if curr_tag_atp_id is None or curr_tag_atp_id != file_uploaded_tag_atp_id:
                transition_to_workflow_status(db_session, str(ref.reference_id), mod, file_uploaded_tag_atp_id, transition_type)
                if logger:
                    logger.info(f"Transitioning file_upload workflow_tag to 'file_uploaded' for reference_id = {ref.reference_id}, mod={mod}")
        except Exception as e:
            logger.error(f"An error occurred when transitioning file_upload workflow_tag to 'file_uploaded'. error={e}")
            db_session.rollback()
            return

    db_session.commit()


def add_file_needed_for_new_papers(db_session: Session, mod, curie_or_reference_id=None, transition_type="automated", logger=None):  # pragma: no cover

    if logger:
        logger.info("Adding file_needed tag for new papers...")

    m = db_session.query(ModModel).filter_by(abbreviation=mod).one_or_none()
    if m is None:
        if logger:
            logger.error(f"The mod abbreviation: {mod} is not in the database.")
        return
    mod_id = m.mod_id

    all_workflow_tags_for_process = get_workflow_tags_from_process(file_upload_process_atp_id)

    if all_workflow_tags_for_process is None:
        logger.info(f"returning None for file_upload_process_atp_id: {file_upload_process_atp_id}. Not connected to a right database/API is down?")
        return

    rows = db_session.query(WorkflowTagModel).join(ModModel).filter(
        and_(
            WorkflowTagModel.workflow_tag_id.in_(all_workflow_tags_for_process),
            ModModel.abbreviation == mod
        )
    ).all()
    reference_ids_with_wft = {row.reference_id for row in rows}

    if curie_or_reference_id is None:
        rows = db_session.execute(text(f"SELECT reference_id FROM mod_corpus_association WHERE mod_id = {mod_id} AND corpus is True")).fetchall()
        if rows is None:
            if logger:
                logger.error("Error: query for mod_corpus_association returned None.")
            return
        reference_ids = {row[0] for row in rows}
    else:
        reference_ids = {curie_or_reference_id}

    if logger:
        logger.info(f"Total reference_ids to process: {len(reference_ids)}")
        logger.info(f"Total reference_ids with existing workflow tags: {len(reference_ids_with_wft)}")

    if not reference_ids:
        if logger:
            logger.warning("No reference_ids found to process.")
        return

    try:
        count = 0
        for reference_id in reference_ids:
            if reference_id not in reference_ids_with_wft:
                count += 1
                transition_to_workflow_status(db_session, str(reference_id), mod, file_needed_tag_atp_id, transition_type)
                if logger:
                    logger.info(f"{count} adding 'file_needed' tag for reference_id = {reference_id}")
            if count % batch_size_for_commit == 0:
                db_session.commit()
                # db_session.rollback()
    except Exception as e:
        if logger:
            logger.error(f"An error occurred when adding 'file_needed' tags for new papers. error={e}")
        db_session.rollback()
        return

    db_session.commit()
    # db_session.rollback()


def move_mod_papers_into_corpus(db_session: Session, mod, mod_id, mod_reference_id_set, logger=None):  # pragma: no cover

    try:
        for x in db_session.query(ModCorpusAssociationModel).filter_by(
                mod_id=mod_id).filter(
                    or_(ModCorpusAssociationModel.corpus.is_(False),
                        ModCorpusAssociationModel.corpus.is_(None))).all():
            if x.reference_id in mod_reference_id_set:
                x.corpus = True
                if x.mod_corpus_sort_source != 'dqm_files':
                    x.mod_corpus_sort_source = 'dqm_files'
                db_session.add(x)
                if logger:
                    logger.info(f"Moving {mod} paper into {mod} corpus for reference_id = {x.reference_id}")
    except Exception as e:
        if logger:
            logger.info(f"An error occurred when moving {mod} paper(s) in/out {mod} corpus. error={e}")

    db_session.commit()
    # db_session.rollback()


def change_mod_curie_status(db_session: Session, mod, mod_curie_set, mod_curie_to_pmid, logger=None, chunk_size: int = 200):  # pragma: no cover # noqa: C901

    curie_prefix = mod
    if mod == 'XB':
        curie_prefix = "Xenbase"
    mod_reference_id_set = set()
    mod_curie_set_in_db = set()
    problematic = []
    planned_changes = []
    for x in db_session.query(CrossReferenceModel).filter_by(curie_prefix=curie_prefix).all():
        if x.reference_id is None:
            continue
        if x.curie in mod_curie_set:
            mod_reference_id_set.add(x.reference_id)
            mod_curie_set_in_db.add(x.curie)
            if x.is_obsolete is True:
                planned_changes.append((x.curie, x.reference_id, False))
        elif x.is_obsolete is False:
            if _is_prepublication_pipeline(db_session, x.reference_id):
                continue
            planned_changes.append((x.curie, x.reference_id, True))

    # Helper to apply a single change (refetch object after rollback)
    def _apply_one(curie: str, ref_id: int, new_is_obsolete: bool) -> bool:
        try:
            obj = (
                db_session.query(CrossReferenceModel)
                .filter_by(curie=curie, reference_id=ref_id, curie_prefix=curie_prefix)
                .one_or_none()
            )
            if obj is None:
                return True  # nothing to do; treat as success
            if obj.is_obsolete != new_is_obsolete:
                obj.is_obsolete = new_is_obsolete
                db_session.add(obj)
            db_session.commit()
            if logger:
                state = "valid" if new_is_obsolete is False else "obsolete"
                logger.info(f"Changing {mod} curie to {state} for {curie}")
            return True
        except Exception as e:
            db_session.rollback()
            if logger:
                logger.info(f"Failed updating {mod} curie {curie} (ref {ref_id}): {e}")
            return False

    # Process in chunks; on chunk failure, retry per-item to isolate problematic curies
    for i in range(0, len(planned_changes), chunk_size):
        chunk = planned_changes[i:i + chunk_size]
        try:
            for curie, ref_id, new_is_obsolete in chunk:
                obj = (
                    db_session.query(CrossReferenceModel)
                    .filter_by(curie=curie, reference_id=ref_id, curie_prefix=curie_prefix)
                    .one_or_none()
                )
                if obj is None:
                    continue
                if obj.is_obsolete != new_is_obsolete:
                    obj.is_obsolete = new_is_obsolete
                    db_session.add(obj)

            db_session.commit()
            if logger:
                logger.info(f"{mod}: committed curie status updates chunk {i//chunk_size + 1} "
                            f"({len(chunk)} changes).")

        except Exception as e:
            db_session.rollback()
            if logger:
                logger.warning(f"{mod}: rollback chunk {i//chunk_size + 1} ({len(chunk)} changes): {e}")
                logger.info(f"{mod}: retrying chunk {i//chunk_size + 1} item-by-item to isolate failures.")

            for curie, ref_id, new_is_obsolete in chunk:
                ok = _apply_one(curie, ref_id, new_is_obsolete)
                if not ok:
                    problematic.append(curie)

    if len(problematic) > 0:
        results = fix_problematic_mod_curies(db_session, mod, problematic, mod_curie_to_pmid, logger)
        for c, status in results:
            if logger:
                logger.info(f"{c}: {status}")

    mod_id = _get_mod_id_by_mod(db_session, mod)
    mod_curies_not_in_db = mod_curie_set - mod_curie_set_in_db

    if logger:
        logger.info("Moving mod papers into corpus...")
    move_mod_papers_into_corpus(db_session, mod, mod_id, mod_reference_id_set, logger)

    if logger:
        logger.info("Moving obsolete papers out of corpus...")
    move_obsolete_papers_out_of_corpus(db_session, mod, mod_id, curie_prefix, logger)

    if logger:
        logger.info("Adding mod PubMed papers with a conflict DOI/PMCID into corpus...")
    add_not_loaded_pubmed_papers(db_session, mod, mod_id, mod_curies_not_in_db,
                                 mod_curie_to_pmid, logger)


def _ref_id_by_pmid(db_session: Session, pmid_like: str):  # pragma: no cover
    """Resolve a PMID or 'PMID:####' to reference_id via cross_reference."""
    pmid_curie = pmid_like if pmid_like.startswith("PMID:") else f"PMID:{pmid_like}"
    row = db_session.execute(
        text("SELECT reference_id FROM cross_reference WHERE curie = :c LIMIT 1"),
        {"c": pmid_curie},
    ).fetchone()
    return row[0] if row and row[0] is not None else None


def fix_problematic_mod_curies(db_session: Session, mod: str, problematic_curies: Iterable[str], mod_curie_to_pmid: Dict[str, str], logger=None):  # pragma: no cover # noqa: C901
    """
    Here’s how fix_problematic_mod_curies() makes that decision in order:
    For each problematic MOD curie (say FB:FBrf0263191), it looks in mod_curie_to_pmid -
    which came from the DQM file — to see if that curie is linked to a PMID (for example "PMID:39876543").

    If a PMID is found, it runs a small query to resolve that PMID to the reference_id in our database:
    SELECT reference_id FROM cross_reference WHERE curie = 'PMID:39876543'

    That reference_id becomes the target reference that this MOD curie should belong to.

    Among all rows in cross_reference with curie = 'FB:FBrf0263191', it:

    Picks one (the one already pointing to the target reference_id),

    Sets is_obsolete = False for that row,

    And marks any other rows with the same curie is_obsolete = True (unless prepub).

    If no PMID link exists in mod_curie_to_pmid, it falls back to:

    keeping whichever row is currently active, or

    any existing row if all are obsolete.

    For example:

    “FB:FBrf0263191 corresponds to PMID:39876543”
    and our DB’s cross_reference table has
    one row curie='PMID:39876543' → reference_id=1156523,
    then the function will ensure that the row with FB:FBrf0263191 now points to
    reference_id=1156523 and has is_obsolete=False.

    All other rows with that same curie will be marked obsolete, so we end up with one clean,
    active mapping consistent with the DQM + PubMed data.
    """
    curie_prefix = "Xenbase" if mod == "XB" else mod
    results: List[Tuple[str, str]] = []

    for curie in problematic_curies:
        try:
            # Get all rows for this curie under this MOD prefix
            rows = (db_session.query(CrossReferenceModel)
                    .filter_by(curie=curie, curie_prefix=curie_prefix)
                    .all())

            if not rows:
                results.append((curie, "skipped: no rows found"))
                continue

            # Determine target reference_id
            target_ref_id: Optional[int] = None

            # 1) Prefer PMID mapping if available
            pmid = mod_curie_to_pmid.get(curie)
            if pmid:
                target_ref_id = _ref_id_by_pmid(db_session, pmid)

            # 2) Else keep current active owner if any
            if target_ref_id is None:
                active_rows = [r for r in rows if r.is_obsolete is False]
                if active_rows:
                    target_ref_id = active_rows[0].reference_id

            # 3) Else fallback to any row’s ref_id
            if target_ref_id is None:
                target_ref_id = next((r.reference_id for r in rows if r.reference_id is not None), None)

            if target_ref_id is None:
                results.append((curie, "skipped: could not resolve target reference_id"))
                continue

            # Pick a keeper row (prefer one already pointing to target, else any obsolete row, else first)
            keeper = None
            for r in rows:
                if r.reference_id == target_ref_id:
                    keeper = r
                    break
            if keeper is None:
                keeper = next((r for r in rows if r.is_obsolete), rows[0])

            # Re-point keeper to target & activate
            keeper.reference_id = target_ref_id  # type: ignore
            keeper.is_obsolete = False  # type: ignore
            db_session.add(keeper)

            # Obsolete all other rows for this curie (respect prepub)
            for r in rows:
                if r is keeper:
                    continue
                if r.is_obsolete is False:
                    if _is_prepublication_pipeline(db_session, r.reference_id):
                        # leave it alone if prepub
                        continue
                    r.is_obsolete = True
                    db_session.add(r)

            db_session.commit()
            if logger:
                logger.info(f"Fixed {curie}: active ref_id={target_ref_id}, others set obsolete where allowed")
            results.append((curie, f"fixed: active ref_id={target_ref_id}"))

        except Exception as e:
            db_session.rollback()
            if logger:
                logger.warning(f"Failed to fix {curie}: {e}")
            results.append((curie, f"failed: {e}"))

    return results


def add_not_loaded_pubmed_papers(db_session: Session, mod, mod_id, mod_curies_to_load, mod_curie_to_pmid, logger):  # pragma: no cover

    mod_curies_not_in_db = set()
    for curie in mod_curies_to_load:
        if curie in mod_curie_to_pmid:
            pmid = mod_curie_to_pmid[curie]
            rows = db_session.query(CrossReferenceModel).filter_by(curie=pmid).all()
            if len(rows) == 0:
                mod_curies_not_in_db.add(curie)
            else:
                reference_id = None
                for x in rows:
                    if x.is_obsolete is False:
                        reference_id = x.reference_id
                        break
                if reference_id is None:
                    mod_curies_not_in_db.add(curie)
                    continue
                try:
                    cr = CrossReferenceModel(curie_prefix=curie.split(':')[0],
                                             curie=curie,
                                             is_obsolete=False,
                                             reference_id=reference_id)
                    db_session.add(cr)
                    mca = db_session.query(ModCorpusAssociationModel).filter_by(
                        mod_id=mod_id, reference_id=reference_id).one_or_none()
                    if mca is None:
                        mod_curies_not_in_db.add(curie)
                        continue
                    mca.corpus = True
                    mca.mod_corpus_sort_source = "dqm_files"
                    db_session.add(mca)
                    if logger:
                        logger.info(f"Adding {mod} {curie} into the database.")
                    db_session.commit()
                except Exception as e:
                    db_session.rollback()
                    mod_curies_not_in_db.add(curie)
                    if logger:
                        logger.info(f"An error occurred when adding {mod} {curie} into the database. error={e}")
        else:
            mod_curies_not_in_db.add(curie)

    if logger:
        logger.info(f"{mod} curies that are not loaded into the database: {mod_curies_not_in_db}")


def move_obsolete_papers_out_of_corpus(db_session: Session, mod, mod_id, curie_prefix, logger=None):  # pragma: no cover

    cr_rows = db_session.execute(text(f"SELECT reference_id "
                                      f"FROM cross_reference "
                                      f"WHERE curie_prefix = '{curie_prefix}' "
                                      f"AND is_obsolete is False")).fetchall()
    valid_reference_ids = {row[0] for row in cr_rows}

    # NEW: for FB only, detect references that have *any* FB curie at all
    ref_ids_with_any_mod_curie = set()
    if mod == "FB":
        any_rows = db_session.execute(text(f"""
            SELECT DISTINCT reference_id
            FROM cross_reference
            WHERE curie_prefix = '{curie_prefix}'
              AND reference_id IS NOT NULL
        """)).fetchall()
        ref_ids_with_any_mod_curie = {row[0] for row in any_rows}

    mca_rows = db_session.execute(text(f"SELECT mca.mod_corpus_association_id, mca.reference_id "
                                       f"FROM mod_corpus_association mca, reference r "
                                       f"WHERE mca.mod_id = {mod_id} "
                                       f"AND mca.corpus is True "
                                       f"AND mca.reference_id = r.reference_id "
                                       f"AND r.prepublication_pipeline is False")).fetchall()
    try:
        # update those that are no longer valid
        for assoc_id, ref_id in mca_rows:
            if ref_id in valid_reference_ids:
                continue
            # NEW: FB exception — don't move out if there is no FB curie row at all
            if mod == "FB" and ref_id not in ref_ids_with_any_mod_curie:
                if logger:
                    logger.info(f"FB: keeping reference_id={ref_id} in corpus (no FB cross_reference curie exists).")
                continue

            db_session.execute(
                text("""
                    UPDATE mod_corpus_association
                       SET corpus = False
                    WHERE mod_corpus_association_id = :assoc_id
                """),
                {"assoc_id": assoc_id}
            )
            if logger:
                logger.info(
                    f"Moving {mod} paper out of corpus for assoc_id={assoc_id}"
                )
        db_session.commit()
    except Exception as e:
        db_session.rollback()
        if logger:
            logger.error(f"Failed to move {mod} papers out of corpus: {e}")


def _is_prepublication_pipeline(db_session: Session, reference_id):  # pragma: no cover

    rows = db_session.execute(text(f"SELECT prepublication_pipeline "
                                   f"FROM   reference "
                                   f"WHERE  reference_id = {reference_id}")).fetchall()
    return rows[0][0]


def mark_not_in_mod_papers_as_out_of_corpus(mod, missing_papers_in_mod, logger=None):

    db_session = create_postgres_session(False)

    mod_id = _get_mod_id_by_mod(db_session, mod)

    # xref_id: MOD database ID, eg. SGD:00000000005
    for (xref_id, agr, pmid) in missing_papers_in_mod:
        if mod == "FB" and not xref_id:
            continue
        try:
            cr = db_session.query(CrossReferenceModel).filter_by(curie=xref_id, is_obsolete=False).one_or_none()
            if cr and _is_prepublication_pipeline(db_session, cr.reference_id):
                continue
            if cr:
                cr.is_obsolete = True
                db_session.add(cr)
                mca = db_session.query(ModCorpusAssociationModel).filter_by(reference_id=cr.reference_id, mod_id=mod_id).one_or_none()
                if mca:
                    mca.corpus = False
                    db_session.add(mca)
                    print(mod + ": marking", xref_id, "out of corpus")
                    if logger:
                        logger.info(mod + ": marking " + xref_id + "(AGR curie=" + agr + ", PMID=" + str(pmid) + ") as out of corpus")
        except Exception as e:
            if logger:
                logger.info("An error occurred when marking paper " + xref_id + "as out of corpus. error=" + str(e))

    db_session.commit()
    # db_session.rollback()


def mark_false_positive_papers_as_out_of_corpus(db_session: Session, mod, fp_pmids, logger=None):  # noqa: C901

    mod_id = _get_mod_id_by_mod(db_session, mod)

    rows = db_session.execute(text(f"SELECT cr.reference_id, cr.curie "
                                   f"FROM cross_reference cr, mod_corpus_association mca "
                                   f"WHERE cr.curie_prefix = 'PMID' "
                                   f"AND cr.reference_id = mca.reference_id "
                                   f"AND mca.mod_id = {mod_id}")).fetchall()

    to_unlink_reference_id_list = []
    i = 0
    for x in rows:
        reference_id = x[0]
        pmid = x[1].replace("PMID:", "")
        i += 1
        if i % 1000 == 0 and logger:
            logger.info(str(i) + " Retrieving PMIDs from the database...")
        if pmid in fp_pmids:
            to_unlink_reference_id_list.append((pmid, reference_id))

    i = 0
    for (pmid, reference_id) in to_unlink_reference_id_list:
        x = db_session.query(ModCorpusAssociationModel).filter_by(reference_id=reference_id, mod_id=mod_id).one_or_none()
        if x and x.corpus is not False:
            i += 1
            if i % 300 == 0:
                db_session.commit()
            try:
                x.corpus = False
                db_session.add(x)
                print("PMID:" + pmid + " has been marked as out of corpus for " + mod)
                if logger:
                    logger.info("PMID:" + pmid + " has been marked as out of corpus for " + mod)
            except Exception as e:
                if logger:
                    logger.info("An error occurred when marking PMID:" + pmid + " as out of corpus for " + mod + ". error = " + str(e))

    db_session.commit()

    curie_prefix = mod
    if mod == 'XB':
        curie_prefix = 'Xenbase'

    for (pmid, reference_id) in to_unlink_reference_id_list:
        x = db_session.query(CrossReferenceModel).filter_by(reference_id=reference_id, curie_prefix=curie_prefix, is_obsolete=False).one_or_none()
        if x:
            i += 1
            if i % 300 == 0:
                db_session.commit()
            try:
                x.is_obsolete = True
                db_session.add(x)
                print(mod + ": PMID:" + pmid + " set " + x.curie + " to obsolete")
                if logger:
                    logger.info(mod + ": PMID:" + pmid + " set " + x.curie + " to obsolete")
            except Exception as e:
                if logger:
                    logger.info(mod + ": An error occurred when setting PMID:" + pmid + " " + x.curie + " to obsolete. error = " + str(e))

    db_session.commit()


def add_cross_references(cross_references_to_add, ref_curie_list, logger, live_change=True):

    if len(ref_curie_list) == 0:
        return

    db_session = create_postgres_session(False)

    ref_curies = ", ".join(["'" + x + "'" for x in ref_curie_list])
    rs = db_session.execute(text("SELECT reference_id, curie FROM reference WHERE curie IN (" + ref_curies + ")"))
    rows = rs.fetchall()
    curie_to_reference_id = {}
    for x in rows:
        curie_to_reference_id[x[1]] = x[0]

    i = 0
    j = 0
    just_added = set()
    for entry in cross_references_to_add:
        i += 1
        j += 1
        logger.info("Adding cross_reference #%s out of %s ", j, len(cross_references_to_add))
        if i > batch_size_for_commit:
            i = 0
            if live_change:
                db_session.commit()
            else:
                db_session.rollback()

        reference_id = curie_to_reference_id.get(entry['reference_curie'])
        if reference_id is None:
            # it won't happen, but still check
            continue

        if entry["curie"] in just_added:
            logger.info("The XREF: " + entry["curie"] + " is a DUPLICATE one, just added to cross_reference table.")
            continue
        just_added.add(entry["curie"])

        rs = db_session.execute(text("SELECT reference_id, resource_id, is_obsolete FROM cross_reference WHERE curie = '" + entry["curie"] + "'"))
        rows = rs.fetchall()
        if len(rows) > 0:
            for x in rows:
                logger.info("The XREF: " + entry["curie"] + " is already in cross_reference table. It is associated with reference_id = " + str(x[0]) + ", resource_id = " + str(x[1]) + ", is_obsolete=" + str(x[2]))
            continue

        prefix = entry["curie"].split(':')[0]
        status = check_pattern('reference', entry["curie"])
        if status is None:
            logger.info(f"Unable to find curie prefix {prefix} in pattern list for reference")
            continue
        if status is False:
            logger.info(f"The curie {entry['curie']} doesn't match the pattern for reference")
            continue
        try:
            x = CrossReferenceModel(reference_id=reference_id,
                                    curie_prefix=prefix,
                                    curie=entry["curie"],
                                    pages=entry.get("pages"))
            db_session.add(x)
            logger.info("The cross_reference row for reference_id = " + str(reference_id) + " and curie = " + entry["curie"] + " has been added into database.")
        except Exception as e:
            logger.info("An error occurred when adding cross_reference row for reference_id = " + str(reference_id) + " and curie = " + entry["curie"] + " " + str(e))

    if live_change:
        db_session.commit()
    else:
        db_session.rollback()
    db_session.close()


def _write_log_message(reference_id, log_message, pmid, logger, fw):  # pragma: no cover

    if pmid:
        log_message = "PMID:" + str(pmid) + log_message
    else:
        log_message = "REFERENCE_ID:" + str(reference_id) + log_message
    if logger:
        logger.info(log_message)
    elif fw:
        fw.write(log_message + "\n")


def update_authors(db_session: Session, reference_id, author_list_in_db: List[Dict[str, str]], author_list_in_json: List[Dict[str, str]], pub_status_changed: str, pmids_with_pub_status_changed: Dict[str, Dict[str, List]], logger=None, fw=None, pmid=None, update_log=None):  # noqa: C901 # pragma: no cover
    """
    Update authors in DB based on data from PubMed or DQM submission for a single reference

    * Perform case-insensitive comparisons of author names after trimming leading and
      trailing whitespace.
    * Update existing author records to maintain author IDs as much as possible.
    * Handle updates to affiliations and ORCIDs without deleting the entire author record.
    * Update author names if the order remains the same to maintain author IDs.
    * Ensure that author order values are updated while respecting unique constraints.


    TODO: ABC currently does not have any author rows with the first_author or corresponding_author
    tags set to true. We will address authors with these tags and those connected to PERSON
    in the future simultaneously.
    Skip these authors during the update and send a report to the curators.
    """

    if author_list_in_json is None:
        author_list_in_json = []

    authors_from_json = Author.load_list_of_authors_from_json_dict_list(author_list_in_json)
    authors_from_db = Author.load_list_of_authors_from_db_dict_list(author_list_in_db)

    if authors_lists_are_equal(authors_from_json, authors_from_db):
        return []

    # Compute a dynamic offset above any existing order
    max_order = max((a.order for a in authors_from_db), default=0)
    author_offset = max_order + 1

    # create dictionaries of authors for comparison
    # example unique key: ('maita', 'nobuo', 'n', 'nobuo maita')
    authors_in_db_dict = {author.get_unique_key_based_on_names(): author for author in authors_from_db}
    authors_in_json_dict = {author.get_unique_key_based_on_names(): author for author in authors_from_json}

    """
    Step 1: Identified authors as updatable for entries that have the same unique
    author key (last_name, first_name, first_initial, and name).
    """

    keys_in_db = set(authors_in_db_dict.keys())
    keys_in_json = set(authors_in_json_dict.keys())

    keys_in_db_and_json = keys_in_json & keys_in_db  # keys in both db and json, add the related json objs

    author_order_to_update_record = {authors_in_db_dict[key].order: authors_in_json_dict[key]
                                     for key in keys_in_db_and_json}

    keys_only_in_db = keys_in_db - keys_in_json  # keys only in db, add the objects in the list to remove
    author_order_to_delete_record = {authors_in_db_dict[key].order: authors_in_db_dict[key] for key in keys_only_in_db}

    keys_only_in_json = keys_in_json - keys_in_db  # keys only in json, add the related objects
    author_order_to_add_record = {authors_in_json_dict[key].order: authors_in_json_dict[key]
                                  for key in keys_only_in_json}

    if (len(keys_only_in_db) + len(keys_in_db_and_json) != len(keys_in_db)
            or len(keys_only_in_json) + len(keys_in_db_and_json) != len(keys_in_json)):
        for order, author in author_order_to_update_record.items():
            author_order_to_delete_record[order] = author
            author_order_to_add_record[author.order] = author
        author_order_to_update_record = {}

    if author_order_to_delete_record or author_order_to_add_record or author_order_to_update_record:
        if update_log:
            update_log['author_name'] = update_log.get('author_name', 0) + 1
            update_log['pmids_updated'].append(pmid)

    """
    Step 2: Check to see if whole set of author names are changed
    For example, change from LastName FirstInitial to FirstName LastName
         old: ['Keller R',       'Schneider D']
         new: ['Rebecca Keller', 'Dirk Schneider']
    If yes, change every row in the author record from "delete/create" to "update"
    """

    if are_additions_and_deletions_only_format_changes(len(author_list_in_db),
                                                       author_order_to_add_record,
                                                       author_order_to_delete_record):
        author_order_to_update_record = author_order_to_add_record
        author_order_to_add_record = {}
        author_order_to_delete_record = {}

    """
    Step 3: If it is not a whole set change, try to see if any of to-be-delete
    author row can be mapped to one and only one of to-be-added author
    entry based on same ORCID or same LastName FirstInitial
    If yes, remove the entry from the to-be-delete list and to-be-add
    list, and add it to to-update-list
    """

    if len(author_order_to_add_record) > 0 and len(author_order_to_delete_record) > 0:
        old_order_to_new_order_mapping = synchronize_author_lists(author_order_to_add_record,
                                                                  author_order_to_delete_record,
                                                                  pmid)

        for old_order in old_order_to_new_order_mapping:
            new_order = old_order_to_new_order_mapping[old_order]
            author_order_to_update_record[old_order] = author_order_to_add_record[new_order]
            author_order_to_add_record.pop(new_order)
            author_order_to_delete_record.pop(old_order)

    """
    Step 4: Update authors in the author_order_to_update_record
    """

    temp_order_map: Dict[int, int] = {}
    name_updated: List[Tuple[str, str]] = []
    # sort by keys (old_order)
    for old_order, json_author in sorted(author_order_to_update_record.items()):
        temp_order_map, name_updated = update_author_row(db_session, reference_id,
                                                         old_order, json_author,
                                                         pmid, temp_order_map,
                                                         name_updated, author_offset,
                                                         fw, logger)

    """
    Step 5: Delete author rows in the author_order_to_delete_record
    """
    name_removed: List[str] = []
    for author_order in sorted(author_order_to_delete_record.keys()):
        x = db_session.query(AuthorModel).filter_by(
            reference_id=reference_id, order=author_order).one_or_none()
        if x:
            name_removed.append(x.name)
            db_session.delete(x)

    """
    Step 6: Insert new author rows in the author_order_to_add_record
    """
    name_added = insert_authors(db_session, reference_id, pmid, author_order_to_add_record, fw, logger)

    """
    Step 7: Resolve any order conflicts - just in case the "order" in JSON is not unique
    """
    if len(temp_order_map.values()) != len({order for order in temp_order_map.values()}):
        db_session.rollback()
        raise ValueError("Duplicate order value detected")

    """
    Step 8: Set the author orders (from the temp orders) to the ones in json
    """
    for author_id, new_order in temp_order_map.items():
        x = db_session.query(AuthorModel).filter_by(author_id=author_id).one_or_none()
        if x and x.order != new_order:
            x.order = new_order
            db_session.add(x)

    """
    step 9: logging update summary
    """
    if len(name_added) > 0 or len(name_updated) > 0 or len(name_removed) > 0:
        author_update_messages = []
        if len(name_added) > 0 or len(name_removed) > 0:
            name_list_removed = ', '.join(name_removed)
            name_list_added = ', '.join(name_added)
            author_update_messages.append(("authors", f"Deleted: {name_list_removed}", f"Inserted: {name_list_added}"))
        old_name_list: List[str] = []
        new_name_list: List[str] = []
        for (old_name, new_name) in name_updated:
            old_name_list.append(old_name)
            new_name_list.append(new_name)
        if len(old_name_list) > 0 or len(new_name_list) > 0:
            name_list_old = ', '.join(old_name_list)
            name_list_new = ', '.join(new_name_list)
            author_update_messages.append(("authors", name_list_old, name_list_new))
        status_changed = pmids_with_pub_status_changed.get(pub_status_changed, {})
        data_changed = status_changed.get(pmid, [])
        data_changed = data_changed + author_update_messages
        status_changed[pmid] = data_changed
        pmids_with_pub_status_changed[pub_status_changed] = status_changed
    """
    will return any authors with author data need to be updated AND these authors are
    connected to PERSON OR have first_author flag/corresponding_author flag set to true
    """
    return []


def insert_authors(db_session: Session, reference_id, pmid, author_order_to_add_record, fw, logger):  # pragma: no cover

    name_added: List[str] = []
    author: Author
    sorted_orders = sorted(author_order_to_add_record.keys())
    for order in sorted_orders:
        author = author_order_to_add_record[order]
        try:
            x = AuthorModel(
                reference_id=reference_id,
                name=author.name,
                first_name=author.first_name,
                last_name=author.last_name,
                first_initial=author.first_initial,
                order=author.order,
                affiliations=author.affiliations,
                orcid=author.orcid,
                first_author=False,
                corresponding_author=False
            )
            db_session.add(x)
            log_message = f": INSERT AUTHOR: {author.name} | '{author.affiliations}'"
            _write_log_message(reference_id, log_message, pmid, logger, fw)
            name_added.append(author.name)
        except Exception as e:
            log_message = f": INSERT AUTHOR: {author.name} failed: {str(e)}"
            _write_log_message(reference_id, log_message, pmid, logger, fw)

    return name_added


def update_author_row(db_session: Session, reference_id, author_order, json_author: Author, pmid, temp_order_map, name_updated, author_offset, fw, logger):  # pragma: no cover

    x = db_session.query(AuthorModel).filter_by(
        reference_id=reference_id, order=author_order).one_or_none()
    if x is None:
        return temp_order_map, name_updated
    try:
        if x.name != json_author.name:
            name_updated.append((x.name, json_author.name))
            x.name = json_author.name
        if x.first_name != json_author.first_name:
            x.first_name = json_author.first_name
        if x.last_name != json_author.last_name:
            x.last_name = json_author.last_name
        if x.first_initial != json_author.first_initial:
            x.first_initial = json_author.first_initial
        if x.affiliations != json_author.affiliations:
            x.affiliations = json_author.affiliations
        if x.orcid != json_author.orcid:
            x.orcid = json_author.orcid
        if x.order != json_author.order:
            tmp_order = x.order + author_offset
            x.order = tmp_order
            temp_order_map[x.author_id] = json_author.order
        db_session.add(x)
        log_message = f": UPDATE AUTHOR for {x.name} | {x.affiliations}"
        _write_log_message(reference_id, log_message, pmid, logger, fw)
    except Exception as e:
        log_message = f": UPDATE AUTHOR for {x.name} failed: {str(e)}"
        _write_log_message(reference_id, log_message, pmid, logger, fw)

    return temp_order_map, name_updated


def synchronize_author_lists(author_order_to_add_record, author_order_to_delete_record, pmid):  # pragma: no cover

    old_order_to_new_order_mapping = {}
    for new_order, new_author in author_order_to_add_record.items():
        new_name_key = new_author.get_key_based_on_unaccented_names()
        found_match_old_orders = []
        debugging_old_names = []
        for old_author in author_order_to_delete_record.values():
            if old_author.orcid and old_author.orcid == new_author.orcid:
                found_match_old_orders = [old_author.order]
                break
            old_name_key = old_author.get_key_based_on_unaccented_names()
            debugging_old_names.append(old_name_key)
            if old_name_key == new_name_key:
                found_match_old_orders.append(old_author.order)
            elif ' ' not in old_name_key or ' ' not in new_name_key:
                # for the cases: name = last_name
                if old_name_key.split(' ')[0] == new_name_key.split(' ')[0]:
                    found_match_old_orders.append(old_author.order)
        if len(found_match_old_orders) == 1:
            old_order = found_match_old_orders[0]
            old_order_to_new_order_mapping[old_order] = new_order
        else:
            print(f"PMID:{pmid} NO MATCH found for '{new_name_key}' in ", debugging_old_names)

    return old_order_to_new_order_mapping


def are_additions_and_deletions_only_format_changes(author_count_db, author_order_to_add_record, author_order_to_delete_record):  # pragma: no cover
    """
    check if every pair of corresponding old and new name has the same last name.
    normalize every name to remove accents and convert to lowercase before comparing.
    """

    """
    old: ['Guadalupe-Medina V', 'Wisselink HW', 'Luttik MA', 'de Hulster E',
          'Daran JM', 'Pronk JT', 'van Maris AJ']
    new: ['Víctor Guadalupe-Medina', 'H Wouter Wisselink', 'Marijke Ah Luttik', 'Erik de Hulster',
          'Jean-Marc Daran', 'Jack T Pronk', 'Antonius Ja van Maris']

    old: ['Pillai RS', ' Will CL', ' Luhrmann R', ' Schumperli D', ' Muller B']
    new: ['R S Pillai', 'C L Will', 'R Lührmann', 'D Schümperli', 'B Müller']

    old: ['Keller R', 'Schneider D']
    new: ['Rebecca Keller', 'Dirk Schneider']

    old: ['Kurat CF', 'Recht J', 'Radovani E', 'Durbic T', 'Andrews B', 'Fillingham J']
    new: ['Christoph F Kurat', 'Judith Recht', 'Ernest Radovani', 'Tanja Durbic', 'Brenda Andrews',
          'Jeffrey Fillingham']

    old: ['T Kutateladze']
    new: ['T G Kutateladze']

    old: ['Santos AL', 'Preta G']
    new: ['Ana L Santos', 'Giulio Preta']

    old: ['Zhu YH', 'Zhang C', 'Liu Y', 'Omenn GS', 'Freddolino PL', 'Yu DJ', 'Zhang Y']
    new: ['Yi-Heng Zhu', 'Chengxin Zhang', 'Yan Liu', 'Gilbert S Omenn', 'Peter L Freddolino',
          'Dong-Jun Yu', 'Yang Zhang']
    """

    if len(author_order_to_add_record) > 0 and len(author_order_to_delete_record) > 0:
        sorted_author_orders = sorted(author_order_to_add_record.keys())
        if author_count_db == len(author_order_to_add_record) and author_count_db == len(author_order_to_delete_record):
            for author_order in sorted_author_orders:
                json_author = author_order_to_add_record[author_order]
                db_author = author_order_to_delete_record.get(author_order)
                if db_author is None:
                    return False
                if (db_author.get_key_based_on_unaccented_names() != json_author.get_key_based_on_unaccented_names()
                        and not authors_have_same_name(db_author, json_author)):
                    return False
            return True
    return False


def update_mod_corpus_associations(db_session: Session, mod_to_mod_id, reference_id, mod_corpus_association_db, mod_corpus_association_json, logger):

    db_mod_corpus_association: Dict[str, Dict[str, int | bool]] = {}
    for db_mca_entry in mod_corpus_association_db:
        if db_mca_entry.get('mod') is None or db_mca_entry['mod'].get('abbreviation') is None:
            continue
        mod = db_mca_entry['mod']['abbreviation']
        if mod not in db_mod_corpus_association:
            db_mod_corpus_association[mod] = {}
        db_mod_corpus_association[mod]['id'] = db_mca_entry['mod_corpus_association_id']
        db_mod_corpus_association[mod]['corpus'] = db_mca_entry['corpus']

    for json_mca_entry in mod_corpus_association_json:
        if json_mca_entry.get('mod_abbreviation') is None:
            continue
        mod = json_mca_entry['mod_abbreviation']
        if mod not in db_mod_corpus_association:
            logger.info(json_mca_entry)
            try:
                x = ModCorpusAssociationModel(reference_id=reference_id,
                                              mod_id=mod_to_mod_id[mod],
                                              corpus=json_mca_entry['corpus'],
                                              mod_corpus_sort_source=json_mca_entry['mod_corpus_sort_source'])
                db_session.add(x)
                logger.info("The mod_corpus_association row for reference_id = " + str(reference_id) + " and mod = " + mod + " has been added into database.")
            except Exception as e:
                logger.info("An error occurred when adding mod_corpus_association row for reference_id = " + str(reference_id) + " and mod = " + mod + ". " + str(e))
                return
        elif json_mca_entry['corpus'] != db_mod_corpus_association[mod]['corpus']:
            mod_corpus_association_id = db_mod_corpus_association[mod]['id']
            try:
                db_session.query(ModCorpusAssociationModel).filter_by(mod_corpus_association_id=mod_corpus_association_id).update({"mod_corpus_sort_source": json_mca_entry['mod_corpus_sort_source'], "corpus": json_mca_entry['corpus']})
                logger.info("The mod_corpus_association row for mod_corpus_association_id = " + str(mod_corpus_association_id) + " has been updated in the database.")
            except Exception as e:
                logger.info("An error occurred when updating mod_corpus_association row for mod_corpus_association_id = " + str(mod_corpus_association_id) + " " + str(e))
                return
        if mod == "ZFIN" and json_mca_entry.get("corpus"):
            add_zfin_pre_indexing_tag(db_session, reference_id, mod_to_mod_id[mod], logger)


def add_zfin_pre_indexing_tag(db, reference_id, mod_id, logger):
    try:
        # all ZFIN pre-indexing–style tags
        allowed_tags = [
            'ATP:0000303',
            'ATP:0000304',
            'ATP:0000305',
            'ATP:0000306'
        ]
        existing = (
            db.query(WorkflowTagModel)
              .filter(
                  WorkflowTagModel.reference_id == reference_id,
                  WorkflowTagModel.mod_id == mod_id,
                  WorkflowTagModel.workflow_tag_id.in_(allowed_tags)).first()
        )
        if existing is None:
            atpid = 'ATP:0000306'
            x = WorkflowTagModel(
                reference_id=reference_id,
                mod_id=mod_id,
                workflow_tag_id=atpid
            )
            db.add(x)
            db.commit()
            logger.info(f"Adding ZFIN pre-indexing tag: {atpid}")
        else:
            logger.info(f"ZFIN pre-indexing tag already exists: {existing.workflow_tag_id}")
    except Exception as e:
        logger.error(f"Error when adding ZFIN pre-indexing tag: {e}")


def update_mod_reference_types(db_session: Session, reference_id, db_mod_ref_types, json_mod_ref_types, pubmed_types, logger):  # noqa: C901

    db_mrt_data: Dict[str, Dict[str, int]] = {}
    to_delete_duplicate_rows = []
    for mrt in db_mod_ref_types:
        source = mrt['mod_referencetype']['mod']['abbreviation']
        ref_type = mrt['mod_referencetype']['referencetype']['label']
        mrt_id = mrt['reference_mod_referencetype_id']
        if source not in db_mrt_data:
            db_mrt_data[source] = {}
        if ref_type not in db_mrt_data[source]:
            db_mrt_data[source][ref_type] = mrt_id
        else:
            to_delete_duplicate_rows.append((mrt_id, ref_type))

    json_mrt_data: Dict[str, List[str]] = {}
    referenceTypes = set()
    referenceTypesConflict = set()
    meeting_abstract_present = any(x['referenceType'] == 'Meeting_abstract' for x in json_mod_ref_types)
    for mrt in json_mod_ref_types:
        source = mrt['source']
        ref_type_label = mrt['referenceType']
        if ref_type_label in referenceTypes:
            continue
        if meeting_abstract_present and mrt['referenceType'] in ["Experimental", "Not_experimental"]:
            referenceTypesConflict.add(mrt['referenceType'])
            continue
        referenceTypes.add(ref_type_label)
        if source not in json_mrt_data:
            json_mrt_data[source] = []
        # just in case there is any duplicate in json
        if ref_type_label not in json_mrt_data[source]:
            json_mrt_data[source].append(ref_type_label)

    for mod in json_mrt_data:
        lc_json = [x.lower() for x in json_mrt_data[mod] if x]
        lc_db: Set[str] = set()
        if mod in db_mrt_data:
            lc_json = [x.lower() for x in json_mrt_data[mod] if x]
        for ref_type_label in json_mrt_data[mod]:
            if ref_type_label and ref_type_label.lower() not in lc_db:
                try:
                    insert_mod_reference_type_into_db(db_session, pubmed_types, mod, ref_type_label, reference_id)
                    logger.info("The mod_reference_type for reference_id = " + str(reference_id) + " has been added into the database.")
                except Exception as e:
                    logger.info("An error occurred when adding mod_reference_type row for reference_id = " + str(reference_id) + " has been added into the database. " + str(e))

        if len(lc_db) == 0:
            continue
        for ref_type in db_mrt_data[mod]:
            if ref_type.lower() not in lc_json:
                mod_reference_type_id = db_mrt_data[mod][ref_type]
                if mod_reference_type_id not in to_delete_duplicate_rows:
                    to_delete_duplicate_rows.append((mod_reference_type_id, ref_type))

    for row in to_delete_duplicate_rows:
        (mod_reference_type_id, ref_type) = row
        try:
            x = db_session.query(ReferenceModReferencetypeAssociationModel).filter_by(
                reference_mod_referencetype_id=mod_reference_type_id).one_or_none()
            if x:
                db_session.delete(x)
                logger.info("The mod_reference_type for mod_reference_type_id = " + str(mod_reference_type_id) + " has been deleted from the database.")
        except Exception as e:
            logger.info("An error occurred when deleting mod_reference_type row for mod_reference_type_id = " + str(mod_reference_type_id) + " has been deleted from the database. " + str(e))

    if len(referenceTypesConflict) > 0:
        referenceTypesConflict.add('Meeting_abstract')


def _get_mod_id_by_mod(db_session: Session, mod):  # pragma: no cover

    m = db_session.query(ModModel).filter_by(abbreviation=mod).one_or_none()

    return m.mod_id


def add_mca_to_existing_references(db_session: Session, agr_curies_to_corpus, mod, logger):

    mod_id = _get_mod_id_by_mod(db_session, mod)

    curie_to_reference_id = {}
    for x in db_session.query(ReferenceModel).filter(
            ReferenceModel.curie.in_(agr_curies_to_corpus)).all():
        curie_to_reference_id[x.curie] = x.reference_id

    for curie in agr_curies_to_corpus:
        try:
            reference_id = curie_to_reference_id[x.curie]
            mca = db_session.query(ModCorpusAssociationModel).filter_by(reference_id=reference_id, mod_id=mod_id).all()
            if len(mca) > 0:
                continue
            mca = ModCorpusAssociationModel(reference_id=reference_id,
                                            mod_id=mod_id,
                                            mod_corpus_sort_source='mod_pubmed_search',
                                            corpus=None)
            db_session.add(mca)
            logger.info("The mod_corpus_association row has been added into database for mod = " + mod + ", reference_curie = " + curie)
        except Exception as e:
            logger.info("An error occurred when adding mod_corpus_association for mod = " + mod + ", reference_curie = " + curie + ". error = " + str(e))

    db_session.commit()


def check_handle_duplicate(db_session: Session, mod, pmids, xref_ref, ref_xref_valid, logger):  # noqa: C901 # pragma: no cover

    # check for papers with same doi in the database
    # print ("ref_xref_valid=", str(ref_xref_valid['AGR:AGR-Reference-0000167781']))
    # ref_xref_valid= {'DOI': '10.1111/j.1440-1711.2005.01311.x', 'MGI': '3573820', 'PMID': '15748210'}
    # print ("xref_ref['DOI'][doi]=", str(xref_ref['DOI']['10.1111/j.1440-1711.2005.01311.x']))
    # xref_ref['DOI'][doi]= AGR:AGR-Reference-0000167781

    from datetime import datetime
    base_path = environ.get('XML_PATH', '')
    json_path = base_path + "pubmed_json/"
    log_path = base_path + 'pubmed_search_logs/'
    log_url = None
    if environ.get('LOG_PATH'):
        log_path = path.join(environ['LOG_PATH'], 'pubmed_search/')
        if environ.get('LOG_URL'):
            log_url = path.join(environ['LOG_URL'], 'pubmed_search/')
    if not path.exists(log_path):
        makedirs(log_path)
    log_file = log_path + "duplicate_rows_" + mod + ".log"
    fw = None
    if path.exists(log_file):
        fw = open(log_file, "a")
    else:
        fw = open(log_file, "w")
    not_loaded_pmids = []
    for pmid in pmids:
        json_file = json_path + pmid + ".json"
        if not path.exists(json_file):
            continue
        f = open(json_file)
        json_data = json.load(f)
        f.close()
        cross_references = json_data['crossReferences']
        xref_ids = []
        for c in cross_references:
            if c['id'].startswith('DOI:') or c['id'].startswith('PMCID:'):
                xref_ids.append(c['id'])
        for xref_id in xref_ids:
            is_doi = False
            is_pmcid = False
            if xref_id.startswith('DOI:'):
                is_doi = True
                xref_id = xref_id.replace("DOI:", '')
            if xref_id.startswith('PMCID:'):
                is_pmcid = True
                xref_id = xref_id.replace("PMCID:", '')
            agr = None
            if is_doi is True and xref_id in xref_ref['DOI']:
                agr = xref_ref['DOI'][xref_id]
            elif is_pmcid is True and xref_id in xref_ref['PMCID']:
                agr = xref_ref['PMCID'][xref_id]
            if agr is None:
                continue
            ## this xref_id (DOI or PMCID) is in the database
            all_ref_xref = ref_xref_valid[agr] if agr in ref_xref_valid else {}
            # found pmids for this DOI or PMCID
            found_pmids_for_this_xref_id = []
            for prefix in all_ref_xref:
                if prefix == 'PMID':
                    if type(all_ref_xref[prefix]) is set:
                        for x in all_ref_xref[prefix]:
                            found_pmids_for_this_xref_id.append(x)
                    else:
                        found_pmids_for_this_xref_id.append(all_ref_xref[prefix])
            if len(found_pmids_for_this_xref_id) == 0:
                reference_id = get_reference_id_by_curie(db_session, agr)
                if reference_id is None:
                    logger.info("The reference curie: " + agr + " is not in the database.")
                try:
                    cross_ref = CrossReferenceModel(curie="PMID:" + pmid,
                                                    curie_prefix='PMID',
                                                    reference_id=reference_id)
                    db_session.add(cross_ref)
                    fw.write(str(datetime.now()) + ": adding PMID:" + pmid + " to the row with XREF_ID = " + xref_id + " in the database\n")
                except Exception as e:
                    logger.info(str(datetime.now()) + ": adding " + pmid + " to the row with " + xref_id + " is failed: " + str(e) + "\n")
            else:
                fw.write(str(datetime.now()) + ": " + xref_id + " for PMID:" + pmid + " is associated with PMID(s) in the database: " + ",".join(found_pmids_for_this_xref_id) + "\n")
                not_loaded_pmids.append((pmid, xref_id, ",".join(found_pmids_for_this_xref_id)))
            if pmid in pmids:
                pmids.remove(pmid)
    fw.close()

    return (log_path, log_url, not_loaded_pmids)


def get_reference_relation_rows(db_session: Session, reference_id_from, reference_id_to):  # pragma: no cover
    rows = db_session.query(ReferenceRelationModel).filter(
        or_(
            and_(ReferenceRelationModel.reference_id_from == reference_id_from,
                 ReferenceRelationModel.reference_id_to == reference_id_to),
            and_(ReferenceRelationModel.reference_id_from == reference_id_to,
                 ReferenceRelationModel.reference_id_to == reference_id_from)
        )
    ).all()
    return rows


def _insert_reference_relation(db_session: Session, fw, pmid, reference_id_from, reference_id_to, type):  # pragma: no cover
    # check to see if any newly added ones matches this entry
    rows = get_reference_relation_rows(db_session, reference_id_from, reference_id_to)
    if len(rows) > 0:
        return
    data = {"reference_id_from": reference_id_from,
            "reference_id_to": reference_id_to,
            "reference_relation_type": type}
    try:
        x = ReferenceRelationModel(**data)
        db_session.add(x)
        db_session.commit()
        fw.write("PMID:" + str(pmid) + ": INSERT reference_relations: " + str(reference_id_from) + " " + str(reference_id_to) + " " + type + "\n")
    except Exception as e:
        fw.write("PMID:" + str(pmid) + ": INSERT reference_relations: " + str(reference_id_from) + " " + str(reference_id_to) + " " + type + " failed: " + str(e) + "\n")


def _update_reference_relation(db_session: Session, fw, pmid, reference_id_from, reference_id_to, type):  # pragma: no cover

    all = get_reference_relation_rows(db_session, reference_id_from, reference_id_to)
    if len(all) == 0:
        return

    for x in all:
        db_session.delete(x)
        db_session.commit()
    _insert_reference_relation(db_session, fw, pmid, reference_id_from, reference_id_to, type)


def _delete_reference_relation(db_session: Session, fw, pmid, reference_id_from, reference_id_to, type):  # pragma: no cover

    rows = get_reference_relation_rows(db_session, reference_id_from, reference_id_to)
    for x in rows:
        try:
            db_session.delete(x)
            db_session.commit()
            fw.write("PMID:" + str(pmid) + ": DELETE reference_relations: " + str(reference_id_from) + " " + str(reference_id_to) + " " + type + "\n")
        except Exception as e:
            fw.write("PMID:" + str(pmid) + ": DELETE reference_relations: " + str(reference_id_from) + " " + str(reference_id_to) + " " + type + " failed: " + str(e) + "\n")


def _get_curator_email_who_added_reference_relation(db_session: Session, reference_id_from, reference_id_to, type):  # pragma: no cover

    rows = db_session.execute(text(f"SELECT u.email "
                                   f"FROM reference_relation_version rcc, transaction t, users u "
                                   f"WHERE rcc.reference_id_from = {reference_id_from} "
                                   f"AND rcc.reference_id_to = {reference_id_to} "
                                   f"AND rcc.reference_relation_type = '{type}' "
                                   f"AND rcc.transaction_id = t.id "
                                   f"AND u.id = t.user_id")).mappings().fetchall()
    if len(rows) == 0:
        return None
    for row in rows:
        if row['email'] and '@' in row['email']:
            return row['email']
    return None


def _is_reference_relation_added_by_mod_dqm(db_session: Session, reference_id_from, reference_id_to, type):  # pragma: no cover

    user_id = "sort_dqm_json_reference_updates"
    rows = db_session.execute(text(f"SELECT * "
                                   f"FROM reference_relation_version rcc, transaction t "
                                   f"WHERE rcc.reference_id_from = {reference_id_from} "
                                   f"AND rcc.reference_id_to = {reference_id_to} "
                                   f"AND rcc.reference_relation_type = '{type}' "
                                   f"AND rcc.transaction_id = t.id "
                                   f"AND t.user_id = '{user_id}'")).fetchall()
    if len(rows) > 0:
        return True
    return False


def update_reference_relations(db_session: Session, fw, pmid, reference_id, pmid_to_reference_id, reference_ids_to_reference_relation_type, reference_relation_in_json, update_log):  # noqa: C901

    type_mapping = {'ErratumIn': 'ErratumFor',
                    'CommentIn': 'CommentOn',
                    'RepublishedIn': 'RepublishedFrom',
                    'RetractionIn': 'RetractionOf',
                    'ExpressionOfConcernIn': 'ExpressionOfConcernFor',
                    'ReprintIn': 'ReprintOf',
                    'UpdateIn': 'UpdateOf'}

    if reference_relation_in_json is None or str(reference_relation_in_json) == '{}':
        reference_relation_in_json = {}

    new_reference_ids_to_reference_relation_type = {}
    with db_session.no_autoflush:
        for original_type in reference_relation_in_json:
            other_pmids = reference_relation_in_json[original_type]
            other_reference_ids = []
            for this_pmid in other_pmids:
                other_reference_id = pmid_to_reference_id.get(this_pmid)
                if other_reference_id is None:
                    other_reference_id = get_reference_id_by_pmid(db_session, this_pmid)
                    if other_reference_id is None:
                        continue
                other_reference_ids.append(other_reference_id)
            if len(other_reference_ids) == 0:
                continue
            if any(original_type.endswith(suffix) for suffix in ['For', 'From', 'Of', 'On']):
                reference_id_from = reference_id
                for reference_id_to in other_reference_ids:
                    if reference_id_from != reference_id_to:
                        new_reference_ids_to_reference_relation_type[(reference_id_from, reference_id_to)] = original_type
            else:
                type = type_mapping.get(original_type)
                if type is None:
                    continue
                reference_id_to = reference_id
                for reference_id_from in other_reference_ids:
                    if reference_id_from != reference_id_to:
                        new_reference_ids_to_reference_relation_type[(reference_id_from, reference_id_to)] = type

    # process new and existing reference relations outside the no_autoflush block
    for key in new_reference_ids_to_reference_relation_type:
        type = new_reference_ids_to_reference_relation_type[key]
        (reference_id_from, reference_id_to) = key
        # assuming _update_reference_relation and _insert_reference_relation might involve queries
        with db_session.no_autoflush:
            if key in reference_ids_to_reference_relation_type:
                if reference_ids_to_reference_relation_type[key] == new_reference_ids_to_reference_relation_type[key]:
                    continue
                _update_reference_relation(db_session, fw, pmid, reference_id_from, reference_id_to, type)
                update_log['comment_erratum'] = update_log['comment_erratum'] + 1
                update_log['pmids_updated'].append(pmid)
            else:
                _insert_reference_relation(db_session, fw, pmid, reference_id_from, reference_id_to, type)
                update_log['comment_erratum'] = update_log['comment_erratum'] + 1
                update_log['pmids_updated'].append(pmid)

    # Assuming deletion might also involve queries
    for key in reference_ids_to_reference_relation_type:
        type = reference_ids_to_reference_relation_type[key]
        if key not in new_reference_ids_to_reference_relation_type:
            (reference_id_from, reference_id_to) = key
            with db_session.no_autoflush:
                if reference_id in [reference_id_from, reference_id_to]:
                    ## only remove the ones that are coming from PubMed
                    email = _get_curator_email_who_added_reference_relation(db_session,
                                                                            reference_id_from,
                                                                            reference_id_to,
                                                                            type)
                    is_added_by_dqm_script = _is_reference_relation_added_by_mod_dqm(db_session,
                                                                                     reference_id_from,
                                                                                     reference_id_to,
                                                                                     type)
                    if email or is_added_by_dqm_script:
                        continue
                    _delete_reference_relation(db_session, fw, pmid, reference_id_from, reference_id_to, type)
                    update_log['comment_erratum'] = update_log['comment_erratum'] + 1
                    update_log['pmids_updated'].append(pmid)


def _insert_mesh_term(db_session: Session, fw, pmid, reference_id, terms):  # pragma: no cover

    (heading_term, qualifier_term) = terms

    if qualifier_term == '':
        qualifier_term = None

    data = {'reference_id': reference_id, 'heading_term': heading_term, 'qualifier_term': qualifier_term}
    try:
        x = MeshDetailModel(**data)
        db_session.add(x)
        fw.write("PMID:" + str(pmid) + ": INSERT mesh term: " + str(terms) + "\n")
    except Exception as e:
        fw.write("PMID:" + str(pmid) + ": INSERT mesh term: " + str(terms) + " failed: " + str(e) + "\n")


def _delete_mesh_term(db_session: Session, fw, pmid, reference_id, terms):  # pragma: no cover

    (heading_term, qualifier_term) = terms

    try:
        x = None
        if qualifier_term != '':
            x = db_session.query(MeshDetailModel).filter_by(reference_id=reference_id, heading_term=heading_term, qualifier_term=qualifier_term).one_or_none()
        else:
            for m in db_session.query(MeshDetailModel).filter_by(reference_id=reference_id, heading_term=heading_term).all():
                if not m.qualifier_term:
                    x = m
        if x is None:
            return
        db_session.delete(x)
        fw.write("PMID:" + str(pmid) + ": DELETE mesh term " + str(terms) + "\n")
    except Exception as e:
        fw.write("PMID:" + str(pmid) + ": DELETE mesh term: " + str(terms) + " failed: " + str(e) + "\n")


def update_mesh_terms(db_session: Session, fw, pmid, reference_id, mesh_terms_in_db, mesh_terms_in_json_data, update_log):

    if mesh_terms_in_json_data is None:
        mesh_terms_in_json_data = []

    mesh_terms_in_json = []

    for m in mesh_terms_in_json_data:
        heading_term = m.get('meshHeadingTerm')
        qualifier_term = m.get('meshQualifierTerm', '')
        if heading_term is None:
            continue
        mesh_terms_in_json.append((heading_term, qualifier_term))

    if mesh_terms_in_db is None:
        mesh_terms_in_db = []

    if set(mesh_terms_in_json) == set(mesh_terms_in_db):
        return

    for m in mesh_terms_in_json:
        if m in mesh_terms_in_db:
            continue
        else:
            _insert_mesh_term(db_session, fw, pmid, reference_id, m)

    for m in mesh_terms_in_db:
        if m in mesh_terms_in_json:
            continue
        else:
            _delete_mesh_term(db_session, fw, pmid, reference_id, m)

    update_log['mesh_term'] = update_log['mesh_term'] + 1
    update_log['pmids_updated'].append(pmid)


def _prefix_xref_identifier(identifier, prefix):  # pragma: no cover
    if identifier and not identifier.startswith(prefix):
        return f"{prefix}:{identifier}"
    return identifier


def _check_xref_existence(db_session: Session, model, curie):  # pragma: no cover
    """Checks if an entry exists in the database."""
    return db_session.query(model).filter_by(curie=curie, is_obsolete=False).one_or_none()


def _update_doi(db_session: Session, fw, pmid, reference_id, old_doi, new_doi):  # pragma: no cover

    try:
        new_doi_curie = None
        old_doi_curie = None
        if new_doi:
            new_doi_curie = _prefix_xref_identifier(new_doi, 'DOI')
            x = _check_xref_existence(db_session, CrossReferenceModel, new_doi_curie)
            if x:
                fw.write(f"{new_doi_curie} is already in the database.\n")
                return
        if old_doi:
            old_doi_curie = _prefix_xref_identifier(old_doi, 'DOI')
            x = db_session.query(CrossReferenceModel).filter_by(
                reference_id=reference_id, is_obsolete=False, curie=old_doi_curie).one_or_none()
            if x is None:
                return
            if new_doi_curie:
                x.curie = new_doi_curie
                db_session.add(x)
                fw.write(f"PMID:{pmid}: UPDATE DOI from {old_doi}  to {new_doi}\n")
            else:
                x.is_obsolete = True
                db_session.add(x)
                fw.write(f"PMID:{pmid}: SET DOI:{old_doi} to invalid\n")
    except Exception as e:
        fw.write(f"PMID:{pmid}: UPDATE DOI from {old_doi} to {new_doi} failed: {e}\n")


def _insert_doi(db_session: Session, fw, pmid, reference_id, doi, logger=None):  # pragma: no cover

    ## for some reason, we need to add this check to make sure it is not in db
    doi_curie = _prefix_xref_identifier(doi, 'DOI')
    x = _check_xref_existence(db_session, CrossReferenceModel, doi_curie)
    if x:
        if x.reference_id != reference_id:
            if logger:
                logger.info(f"new {doi_curie} for PMID:{pmid} is associated with another paper in the database: reference_id={x.reference_id}")
        return
    x = db_session.query(CrossReferenceModel).filter_by(
        curie_prefix="DOI", reference_id=reference_id, is_obsolete=False).one_or_none()
    if x:
        if logger:
            logger.info(f"Key (curie_prefix, reference_id)=(DOI, {reference_id}) already exists")
            return
    data = {"curie": "DOI:" + doi,
            "curie_prefix": "DOI",
            "reference_id": reference_id,
            "is_obsolete": False}
    try:
        x = CrossReferenceModel(**data)
        db_session.add(x)
        fw.write(f"PMID:{pmid}: INSERT DOI:{doi}\n")
    except Exception as e:
        fw.write(f"PMID:{pmid}: INSERT DOI:{doi} failed: {e}\n")


def _update_pmcid(db_session: Session, fw, pmid, reference_id, old_pmcid, new_pmcid, logger):  # pragma: no cover

    new_pmcid_curie = None
    old_pmcid_curie = None
    if new_pmcid:
        new_pmcid_curie = _prefix_xref_identifier(new_pmcid, 'PMCID')
        x = _check_xref_existence(db_session, CrossReferenceModel, new_pmcid_curie)
        if x:
            if logger:
                logger.info(f"Key (curie)=(PMCID:{new_pmcid}) already exists for another paper in the database: reference_id={x.reference_id}")
            return
    if old_pmcid:
        old_pmcid_curie = _prefix_xref_identifier(old_pmcid, 'PMCID')
        x = db_session.query(CrossReferenceModel).filter_by(
            reference_id=reference_id, curie=old_pmcid_curie).one_or_none()
        if x is None:
            return
        try:
            if new_pmcid_curie:
                x.curie = new_pmcid_curie
                db_session.add(x)
                fw.write(f"PMID:{pmid}: UPDATE PMCID from {old_pmcid} to {new_pmcid}\n")
            else:
                x.is_obsolete = True
                db_session.add(x)
                fw.write(f"PMID:{pmid}: SET PMCID{old_pmcid} to invalid\n")
        except Exception as e:
            fw.write(f"PMID:{pmid}: UPDATE PMCID from {old_pmcid} to {new_pmcid} failed: {e}\n")


def _insert_pmcid(db_session: Session, fw, pmid, reference_id, pmcid, logger=None):  # pragma: no cover

    ## for some reason, we need to add this check to make sure it is not in db
    curie = _prefix_xref_identifier(pmcid, 'PMCID')
    x = _check_xref_existence(db_session, CrossReferenceModel, curie)
    if x:
        if x.reference_id != reference_id:
            if logger:
                logger.info(f"The new PMCID:{pmcid} for PMID:{pmid} is associated with another paper in the database: reference_id={x.reference_id}")
            return
        if x.reference_id == reference_id:
            return
    x = db_session.query(CrossReferenceModel).filter_by(
        curie_prefix="PMCID", reference_id=reference_id, is_obsolete=False).one_or_none()
    if x:
        if logger:
            logger.info(f"Key (curie_prefix, reference_id)=(PMCID, {reference_id}) already exists")
        return
    x = db_session.query(CrossReferenceModel).filter_by(curie="PMCID:" + pmcid, reference_id=reference_id).one_or_none()
    if x and x.is_obsolete is True:
        x.is_obsolete = False
        db_session.add(x)
        return
    data = {"curie": "PMCID:" + pmcid,
            "curie_prefix": "PMCID",
            "reference_id": reference_id,
            "is_obsolete": False}
    try:
        x = CrossReferenceModel(**data)
        db_session.add(x)
        fw.write(f"PMID:{pmid}: INSERT PMCID:{pmcid}\n")
    except Exception as e:
        fw.write(f"PMID:{pmid}: INSERT PMCID:{pmcid} failed: {e}\n")


def update_cross_reference(db_session: Session, fw, pmid, reference_id, doi_db, doi_list_in_db, doi_json, pmcid_db, pmcid_list_in_db, pmcid_json, pub_status_changed, pmids_with_pub_status_changed, update_log, logger=None):  # pragma: no cover

    if doi_json:
        if doi_json.startswith('10.'):
            doi_json = "DOI:" + doi_json
        status = check_pattern('reference', doi_json)
        if not status:
            logger.info(f"The curie {doi_json} doesn't match the pattern for reference")
            return
    if pmcid_json:
        if pmcid_json.startswith('PMC'):
            pmcid_json = "PMCID:" + pmcid_json
        status = check_pattern('reference', pmcid_json)
        if not status:
            logger.info(f"The curie {pmcid_json} doesn't match the pattern for reference")
            return
    doi_json = doi_json.replace("DOI:", "") if doi_json else None
    pmcid_json = pmcid_json.replace("PMCID:", "") if pmcid_json else None

    ## take care of DOI
    if doi_json and (doi_db is None or doi_json != doi_db) and doi_json in doi_list_in_db:
        fw.write(f"PMID:{pmid}: DOI:{doi_json} is in the database for another paper.\n")
    else:
        if doi_json != doi_db:
            try:
                if doi_db is None:
                    _insert_doi(db_session, fw, pmid, reference_id, doi_json, logger)
                else:
                    _update_doi(db_session, fw, pmid, reference_id, doi_db, doi_json)
                update_log['doi'] = update_log['doi'] + 1
                update_log['pmids_updated'].append(pmid)
            except Exception as e:
                logger.info(str(e))

    ## take care of PMCID
    if pmcid_json:
        if pmcid_json.startswith('PMC'):
            if not pmcid_json.replace('PMC', '').isdigit():
                pmcid_json = None
        else:
            pmcid_json = None

    if pmcid_db == pmcid_json:
        return

    if pmcid_json and (pmcid_db is None or pmcid_json != pmcid_db) and pmcid_json in pmcid_list_in_db:
        fw.write(f"PMID:{pmid}: PMCID:{pmcid_json} is in the database for another paper.\n")
    else:
        if pmcid_db:
            _update_pmcid(db_session, fw, pmid, reference_id, pmcid_db, pmcid_json, logger)
        else:
            _insert_pmcid(db_session, fw, pmid, reference_id, pmcid_json, logger)

        update_log['pmcid'] = update_log['pmcid'] + 1
        update_log['pmids_updated'].append(pmid)

        status_changed = pmids_with_pub_status_changed.get(pub_status_changed, {})
        data_changed = status_changed.get(pmid, [])
        data_changed.append(('PMCID', pmcid_db, pmcid_json))
        status_changed[pmid] = data_changed
        pmids_with_pub_status_changed[pub_status_changed] = status_changed


def insert_referencefile_mod_for_pmc(db_session: Session, pmid, file_name_with_suffix, referencefile_id, logger):  # pragma: no cover

    try:
        x = ReferencefileModAssociationModel(referencefile_id=referencefile_id)
        db_session.add(x)
        logger.info("PMID:" + pmid + ": pmc oa file = " + file_name_with_suffix + ": loaded into Referencefile_mod table")
    except Exception as e:
        logger.info("PMID:" + pmid + ": pmc oa file = " + file_name_with_suffix + ": an error occurred when loading data into Referencefile_modtable: " + str(e))


def insert_referencefile(db_session: Session, pmid, file_class, file_publication_status, file_name_with_suffix, reference_id, md5sum, logger):  # pragma: no cover

    file_extension = file_name_with_suffix.split(".")[-1].lower()
    display_name = file_name_with_suffix.replace("." + file_extension, "")
    pdf_type = None
    if file_extension == 'pdf':
        pdf_type = 'pdf'

    logger.info(file_name_with_suffix + " | " + display_name + " | " + file_extension + " | " + str(pdf_type))

    referencefile_id = None

    try:
        x = ReferencefileModel(display_name=display_name,
                               reference_id=reference_id,
                               md5sum=md5sum,
                               file_class=file_class,
                               file_publication_status=file_publication_status,
                               file_extension=file_extension,
                               pdf_type=pdf_type,
                               is_annotation=False)
        db_session.add(x)
        db_session.flush()
        db_session.refresh(x)
        referencefile_id = x.referencefile_id
        logger.info("PMID:" + pmid + ": pmc oa file = " + file_name_with_suffix + ": loaded into Referencefile table")
    except Exception as e:
        logger.info("PMID:" + pmid + ": pmc oa file = " + file_name_with_suffix + " an error occurred when loading data into Referencefile table. error: " + str(e))

    return referencefile_id


def set_category_for_fb_note_papers(db, logger):
    stmt = text("""
        UPDATE reference
        SET category = 'Comment'
        WHERE reference_id IN (
            SELECT rmrt.reference_id
              FROM reference_mod_referencetype rmrt
              JOIN mod_referencetype mrt
                ON rmrt.mod_referencetype_id = mrt.mod_referencetype_id
              JOIN referencetype rt
                ON mrt.referencetype_id = rt.referencetype_id
              JOIN mod m
                ON mrt.mod_id = m.mod_id
             WHERE rt.label = 'note'
               AND m.abbreviation = 'FB'
        )
          AND reference_id NOT IN (
            SELECT mca.reference_id
              FROM mod_corpus_association mca
              JOIN mod m2
                ON mca.mod_id = m2.mod_id
             WHERE mca.corpus = TRUE
               AND m2.abbreviation <> 'FB'
        )
          AND category <> 'Comment'
    """)
    try:
        result: Result = db.execute(stmt)
        db.commit()
        logger.info(f"Set category='Comment' for {result.rowcount} FB paper(s) with 'note' reference type")
    except Exception as e:
        db.rollback()
        logger.error(f"Failed to set category for FB note papers. Error={e}")
