import copy
import gzip
import hashlib
import logging
import os
import shutil
import tarfile
import tempfile
from itertools import count
from typing import List, Optional, Union

import boto3  # type: ignore
from fastapi import HTTPException, status, UploadFile
from fastapi.encoders import jsonable_encoder
from sqlalchemy import and_, or_, text, select
from sqlalchemy.orm import Session, subqueryload, joinedload
from starlette.background import BackgroundTask
from starlette.responses import FileResponse

from agr_literature_service.api.crud.reference_utils import get_reference
from agr_literature_service.api.crud.referencefile_utils import read_referencefile_db_obj, \
    get_s3_folder_from_md5sum, remove_from_s3_and_db
from agr_literature_service.api.crud.referencefile_mod_utils import create as create_mod_connection, \
    destroy as destroy_mod_association
from agr_literature_service.api.crud.workflow_tag_crud import get_current_workflow_status, \
    transition_to_workflow_status, is_file_upload_blocked, create as create_wft, \
    reset_workflow_tags_after_deleting_main_pdf
from agr_literature_service.api.crud.topic_entity_tag_utils import delete_non_manual_tets, \
    has_manual_tet
from agr_literature_service.api.models import ReferenceModel, ReferencefileModel, \
    ReferencefileModAssociationModel, ModModel, CopyrightLicenseModel, EmbeddingFileModel
from agr_cognito_py import ModAccess, MOD_ACCESS_ABBR
from agr_literature_service.api.s3.upload import upload_file_to_bucket
from agr_literature_service.api.schemas.referencefile_mod_schemas import ReferencefileModSchemaPost
from agr_literature_service.api.schemas.referencefile_schemas import ReferencefileSchemaPost, \
    ReferencefileSchemaRelated, ReferencefileSchemaUpdate
from agr_literature_service.api.schemas.workflow_tag_schemas import WorkflowTagSchemaPost
from agr_literature_service.api.schemas.response_message_schemas import messageEnum
from agr_literature_service.lit_processing.utils.s3_utils import download_file_from_s3
from agr_literature_service.api.crud.reference_utils import normalize_reference_curie
from agr_literature_service.api.crud.user_utils import map_to_user_id

logger = logging.getLogger(__name__)

file_upload_process_atp_id = "ATP:0000140"
file_uploaded_tag_atp_id = "ATP:0000134"
file_upload_in_progress_tag_atp_id = "ATP:0000139"
file_needed_tag_atp_id = "ATP:0000141"
text_conversion_process_atp_id = "ATP:0000161"


# TRANSIENT (SCRUM-6246 figure-metadata backfill): a one-off process may flip
# this so file_upload() persists *auxiliary* files (e.g. the figure-metadata
# JSON sidecars) WITHOUT firing transition_WFT_for_uploaded_file or pruning
# superseded main PDFs via cleanup_old_pdf_file. The reference's file-upload
# workflow status was already set when the figures were originally converted, so
# re-attaching descriptor files must not move it or trigger downstream
# reprocessing. Default is False, so the live API / conversion path is
# unaffected — only the backfill script activates it, for the duration of its
# run (see lit_processing/pdf2md/backfill_figure_metadata.py). Process-level
# (module global): the standalone backfill runs in its own process, so this
# never leaks into the API or cron conversion processes.
_suppress_post_upload_workflow = False


def set_suppress_post_upload_workflow(value: bool) -> None:
    """Toggle suppression of file_upload()'s post-upload workflow side effects
    (transition + main-PDF cleanup). See ``_suppress_post_upload_workflow``."""
    global _suppress_post_upload_workflow
    _suppress_post_upload_workflow = bool(value)


def is_post_upload_workflow_suppressed() -> bool:
    return _suppress_post_upload_workflow


def get_main_pdf_referencefile_id(db: Session, curie_or_reference_id: str,
                                  mod_abbreviation: str = None) -> Union[int, None]:
    logger.info("Getting main pdf referencefile")
    reference: ReferenceModel = get_reference(db=db, curie_or_reference_id=str(curie_or_reference_id),
                                              load_referencefiles=True)
    main_pdf_referencefiles = [referencefile for referencefile in reference.referencefiles if
                               referencefile.file_class == "main" and referencefile.file_publication_status == "final"
                               and referencefile.pdf_type == 'pdf']
    if mod_abbreviation is not None:
        for main_pdf_ref_file in main_pdf_referencefiles:
            for ref_file_mod in main_pdf_ref_file.referencefile_mods:
                if ref_file_mod.mod and ref_file_mod.mod.abbreviation == mod_abbreviation:
                    return main_pdf_ref_file.referencefile_id
    for main_pdf_ref_file in main_pdf_referencefiles:
        for ref_file_mod in main_pdf_ref_file.referencefile_mods:
            if ref_file_mod.mod is None:
                return main_pdf_ref_file.referencefile_id
    return None


def get_main_pdf_referencefile_ids_for_ref_curies_list(db: Session, curies: List[str], mod_abbreviation: str):
    ref_id_curie_map = {ref.reference_id: ref.curie for ref in db.execute(
        select(ReferenceModel.reference_id, ReferenceModel.curie).where(ReferenceModel.curie.in_(curies))
    ).all()}

    all_ref_files = db.execute(
        select(ReferencefileModel)
        .where(ReferencefileModel.reference_id.in_(list(ref_id_curie_map.keys())))
        .options(joinedload(ReferencefileModel.referencefile_mods))
    ).unique().scalars().all()

    curie_main_ref_file_map = {}

    for ref_file in all_ref_files:
        if ref_file.file_class == "main" and ref_file.file_publication_status == "final" and ref_file.pdf_type == "pdf":
            main_pdf_reffile_id = None
            pmc_main_pdf_reffile_id = None
            for ref_file_mod in ref_file.referencefile_mods:
                if ref_file_mod.mod and ref_file_mod.mod.abbreviation == mod_abbreviation:
                    main_pdf_reffile_id = ref_file.referencefile_id
                    break
                if (ref_file_mod.mod is None and main_pdf_reffile_id is None):
                    pmc_main_pdf_reffile_id = ref_file.referencefile_id
            main_pdf_reffile_id = main_pdf_reffile_id or pmc_main_pdf_reffile_id
            if main_pdf_reffile_id:
                curie_main_ref_file_map[ref_id_curie_map[ref_file.reference_id]] = main_pdf_reffile_id
    return curie_main_ref_file_map


def set_referencefile_mods(referencefile_obj, referencefile_dict):
    del referencefile_dict["reference_id"]
    referencefile_dict["referencefile_mods"] = []
    if referencefile_obj.referencefile_mods:
        for ref_file_mod in referencefile_obj.referencefile_mods:
            ref_file_mod_dict = jsonable_encoder(ref_file_mod)
            del ref_file_mod_dict["mod_id"]
            del ref_file_mod_dict["referencefile_id"]
            if ref_file_mod.mod is not None:
                ref_file_mod_dict["mod_abbreviation"] = ref_file_mod.mod.abbreviation
            else:
                ref_file_mod_dict["mod_abbreviation"] = None
            referencefile_dict["referencefile_mods"].append(ref_file_mod_dict)


def show(db: Session, referencefile_id: int):
    referencefile = read_referencefile_db_obj(db, referencefile_id)
    referencefile_dict = jsonable_encoder(referencefile)
    referencefile_dict["reference_curie"] = db.query(ReferenceModel.curie).filter(
        ReferenceModel.reference_id == referencefile_dict["reference_id"]).one()[0]
    set_referencefile_mods(referencefile_obj=referencefile, referencefile_dict=referencefile_dict)
    return referencefile_dict


_CONVERTED_FILE_CLASS_FOR_SOURCE = {
    "main": "converted_merged_main",
    "supplement": "converted_merged_supplement",
    "nXML": "converted_merged_main",
}

# Suffixes appended to a source's display_name by the conversion pipeline
# (pdf2md_utils, the legacy TEI→MD batch, and the nXML→MD path). Kept in
# sync with file_conversion_crud._SUFFIX_TO_SOURCE_CLASS and
# _PDFX_METHOD_SUFFIXES.
_CONVERTED_DISPLAY_NAME_SUFFIXES = (
    "_merged", "_grobid", "_docling", "_marker", "_tei", "_nxml",
)


def _find_converted_derived_for_source(db: Session,
                                       source_ref_file: ReferencefileModel) -> List[dict]:
    """Return converted Markdown referencefiles produced from ``source_ref_file``.

    Uses the display_name suffix convention written by the conversion
    pipeline to map a converted row back to its source PDF/nXML — same
    convention used by ``file_conversion_crud._infer_source_info``.
    """
    converted_file_class = _CONVERTED_FILE_CLASS_FOR_SOURCE.get(source_ref_file.file_class)
    if not converted_file_class:
        return []
    rows = db.query(ReferencefileModel).filter(
        ReferencefileModel.reference_id == source_ref_file.reference_id,
        ReferencefileModel.file_class == converted_file_class,
        ReferencefileModel.file_extension == "md",
        ReferencefileModel.file_publication_status == "final",
    ).all()
    derived: List[dict] = []
    source_display_name = source_ref_file.display_name or ""
    for r in rows:
        display_name = r.display_name or ""
        if not display_name.startswith(source_display_name):
            continue
        suffix = display_name[len(source_display_name):]
        if suffix not in _CONVERTED_DISPLAY_NAME_SUFFIXES:
            continue
        derived.append({
            "referencefile_id": int(r.referencefile_id),
            "display_name": r.display_name,
            "file_class": r.file_class,
            "file_extension": r.file_extension,
        })
    return derived


# Reverse of _CONVERTED_FILE_CLASS_FOR_SOURCE: a converted md's file_class ->
# the set of source PDF/nXML file_classes it may have been produced from.
# Note converted_merged_main can come from a `main` PDF OR an `nXML`, so this
# must be a set (a naive {v: k} reverse would silently drop one source).
_SOURCE_FILE_CLASSES_FOR_CONVERTED: dict = {}
for _src_cls, _conv_cls in _CONVERTED_FILE_CLASS_FOR_SOURCE.items():
    _SOURCE_FILE_CLASSES_FOR_CONVERTED.setdefault(_conv_cls, set()).add(_src_cls)


def _source_dict(rf: ReferencefileModel) -> dict:
    return {
        "referencefile_id": int(rf.referencefile_id),
        "display_name": rf.display_name,
        "file_class": rf.file_class,
        "file_extension": rf.file_extension,
        "md5sum": rf.md5sum,
    }


def _find_source_for_derived(ref_file: ReferencefileModel, all_files: List[ReferencefileModel],
                             embedding_source_by_id: dict) -> Optional[dict]:
    """Resolve the referencefile a derived file was produced from (upward).

    - embedding -> its converted_merged_* md, via the embedding_file FK
      (precomputed in ``embedding_source_by_id``: parquet rf id -> source rf).
    - converted_merged_* md -> its source PDF/nXML, via the display-name
      suffix convention (reverse of _find_converted_derived_for_source).
    Returns the source dict or None when it can't be resolved (e.g. figures,
    source deleted, or convention miss) -- ``source`` is nullable by design.
    """
    if ref_file.file_class == "embedding":
        src = embedding_source_by_id.get(int(ref_file.referencefile_id))
        return _source_dict(src) if src is not None else None
    source_classes = _SOURCE_FILE_CLASSES_FOR_CONVERTED.get(ref_file.file_class)
    if not source_classes:
        return None
    display_name = ref_file.display_name or ""
    for suffix in _CONVERTED_DISPLAY_NAME_SUFFIXES:
        if display_name.endswith(suffix):
            base = display_name[: len(display_name) - len(suffix)]
            for cand in all_files:
                if cand.file_class in source_classes and cand.display_name == base:
                    return _source_dict(cand)
    return None


def get_referencefiles_by_md5(db: Session, md5sum: str) -> List[dict]:
    """Look up every referencefile with the given MD5 checksum.

    Supports the PDF-only ingestion flow (SCRUM-6055): given an MD5 the
    client computed from an uploaded PDF, return any matching referencefiles
    together with the reference curie, MOD associations, open-access /
    license info, and (for source files) the converted Markdown rows
    derived from that same source. A single MD5 may resolve to multiple
    referencefiles when the same content is attached to more than one
    reference.
    """
    ref_files = (
        db.query(ReferencefileModel)
        .filter(ReferencefileModel.md5sum == md5sum)
        .options(
            joinedload(ReferencefileModel.referencefile_mods).joinedload(
                ReferencefileModAssociationModel.mod
            ),
            joinedload(ReferencefileModel.reference).joinedload(
                ReferenceModel.copyright_license
            ),
        )
        .all()
    )
    results: List[dict] = []
    for ref_file in ref_files:
        reference = ref_file.reference
        copyright_license = reference.copyright_license if reference else None
        open_access = bool(copyright_license and copyright_license.open_access)
        copyright_license_name = copyright_license.name if copyright_license else None

        mods_payload: List[dict] = []
        for ref_file_mod in ref_file.referencefile_mods:
            mods_payload.append({
                "referencefile_mod_id": ref_file_mod.referencefile_mod_id,
                "mod_abbreviation": (
                    ref_file_mod.mod.abbreviation if ref_file_mod.mod is not None else None
                ),
                "date_created": ref_file_mod.date_created,
                "date_updated": ref_file_mod.date_updated,
                "created_by": ref_file_mod.created_by,
                "updated_by": ref_file_mod.updated_by,
            })

        results.append({
            "referencefile_id": ref_file.referencefile_id,
            "reference_id": ref_file.reference_id,
            "reference_curie": reference.curie,
            "display_name": ref_file.display_name,
            "file_class": ref_file.file_class,
            "file_publication_status": ref_file.file_publication_status,
            "file_extension": ref_file.file_extension,
            "pdf_type": ref_file.pdf_type,
            "md5sum": ref_file.md5sum,
            "is_annotation": bool(ref_file.is_annotation),
            "open_access": open_access,
            "copyright_license_name": copyright_license_name,
            "date_created": ref_file.date_created,
            "date_updated": ref_file.date_updated,
            "created_by": ref_file.created_by,
            "updated_by": ref_file.updated_by,
            "referencefile_mods": mods_payload,
            "converted_referencefiles": _find_converted_derived_for_source(
                db, ref_file
            ),
        })
    return results


def show_all(db: Session, curie_or_reference_id: str) -> List[ReferencefileSchemaRelated]:
    """Return metadata for EVERY referencefile associated with the reference,
    including `embedding` parquet rows (each annotated with its embedding_file
    catalog fields + source lineage). It is a flat metadata list, so any
    per-file-class / profile / version narrowing is done downstream on the
    result rather than via endpoint params."""
    logger.info("Show all referencefiles")
    reference = get_reference(db=db, curie_or_reference_id=curie_or_reference_id, load_referencefiles=True)
    all_files = list(reference.referencefiles or [])

    # Map embedding parquet referencefile_id -> its embedding_file row, so we
    # can both filter and enrich embedding entries without per-row queries.
    parquet_ids = [int(rf.referencefile_id) for rf in all_files if rf.file_class == "embedding"]
    emb_by_parquet: dict = {}
    source_by_parquet: dict = {}
    if parquet_ids:
        rf_by_id = {int(rf.referencefile_id): rf for rf in all_files}
        emb_rows = db.query(EmbeddingFileModel).filter(
            EmbeddingFileModel.parquet_referencefile_id.in_(parquet_ids)).all()
        for emb_row in emb_rows:
            emb_by_parquet[int(emb_row.parquet_referencefile_id)] = emb_row
            if emb_row.source_referencefile_id is not None:
                source_by_parquet[int(emb_row.parquet_referencefile_id)] = rf_by_id.get(
                    int(emb_row.source_referencefile_id))

    reference_files = []
    for ref_file in all_files:
        is_embedding = ref_file.file_class == "embedding"
        emb_row = emb_by_parquet.get(int(ref_file.referencefile_id)) if is_embedding else None
        ref_file_dict = jsonable_encoder(ref_file)
        set_referencefile_mods(referencefile_obj=ref_file, referencefile_dict=ref_file_dict)
        ref_file_dict["source"] = _find_source_for_derived(ref_file, all_files, source_by_parquet)
        if is_embedding and emb_row is not None:
            ref_file_dict["profile_name"] = emb_row.profile_name
            ref_file_dict["version"] = emb_row.version
            ref_file_dict["model_name"] = emb_row.model_name
        reference_files.append(ref_file_dict)
    return reference_files


def check_file_upload_status_change(db, referencefile, request):

    request_file_class = request.get("file_class", referencefile.file_class)
    request_publication_status = request.get("file_publication_status", referencefile.file_publication_status)
    request_pdf_type = request.get("pdf_type", referencefile.pdf_type)
    change_if_already_converted = request.get("change_if_already_converted", False)

    if (
        referencefile.file_class == 'main'
        and referencefile.file_publication_status == 'final'
        and referencefile.pdf_type == 'pdf'
    ) and (
        request_file_class != 'main'
        or request_publication_status != 'final'
        or request_pdf_type != 'pdf'
    ):
        if not change_if_already_converted:
            for referenceMod in referencefile.referencefile_mods:
                if referenceMod.mod_id:
                    workflow_tag_atp_id = get_current_workflow_status(db,
                                                                      referencefile.reference.curie,
                                                                      text_conversion_process_atp_id,
                                                                      referenceMod.mod.abbreviation)
                    if workflow_tag_atp_id == "ATP:0000163":  # file converted to text
                        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                            detail=f"File already converted to text for {referenceMod.mod.abbreviation}, use UI if you really need to change the file status.")
        return True

    if (
        request_file_class == 'main'
        and request_publication_status == 'final'
        and request_pdf_type == 'pdf'
    ) and (
        referencefile.file_class != 'main'
        or referencefile.file_publication_status != 'final'
        or referencefile.pdf_type != 'pdf'
    ):
        return True
    return False


def patch(db: Session, referencefile_id: int, request):
    referencefile: ReferencefileModel = read_referencefile_db_obj(db, referencefile_id)
    if "display_name" in request or "file_extension" in request or "reference_curie" in request:
        if "display_name" not in request:
            request["display_name"] = referencefile.display_name
        if "file_extension" not in request:
            request["file_extension"] = referencefile.file_extension
        if "reference_curie" not in request:
            request["reference_curie"] = referencefile.reference.curie
        request["display_name"] = find_first_available_display_name(display_name=request["display_name"],
                                                                    file_extension=request["file_extension"],
                                                                    reference_curie=request["reference_curie"], db=db)
    if "created_by" in request and request["created_by"] is not None:
        request["created_by"] = map_to_user_id(request["created_by"], db)
    if "updated_by" in request and request["updated_by"] is not None:
        request["updated_by"] = map_to_user_id(request["updated_by"], db)

    if "reference_curie" in request:
        res = db.query(ReferenceModel.reference_id).filter(
            ReferenceModel.curie == request["reference_curie"]).one_or_none()
        if res is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                detail=f"Reference with curie {request.reference_curie} is not available")
        request["reference_id"] = res[0]
        del request["reference_curie"]
    change_status = check_file_upload_status_change(db, referencefile, request)
    if change_status:
        transition_WFT_for_uploaded_file(db, referencefile.reference.curie, None,
                                         request.get("file_class", referencefile.file_class),
                                         request.get("pdf_type", referencefile.pdf_type),
                                         request.get("file_publication_status", referencefile.file_publication_status),
                                         True)
    for field, value in request.items():
        setattr(referencefile, field, value)
    db.commit()
    return {"message": messageEnum.updated}


def destroy(db: Session, referencefile_id: int, mod_access: ModAccess):
    referencefile: ReferencefileModel = read_referencefile_db_obj(db, referencefile_id)
    reference_id = referencefile.reference_id
    file_class = referencefile.file_class
    file_publication_status = referencefile.file_publication_status
    pdf_type = referencefile.pdf_type
    all_mods = set()
    if mod_access == ModAccess.ALL_ACCESS:
        remove_from_s3_and_db(db, referencefile)
    elif mod_access != ModAccess.NO_ACCESS:
        for referencefile_mod in referencefile.referencefile_mods:
            if referencefile_mod.mod_id is None:
                all_mods.add('PMC')
            else:
                all_mods.add(referencefile_mod.mod.abbreviation)
                if referencefile_mod.mod.abbreviation == MOD_ACCESS_ABBR[mod_access]:
                    destroy_mod_association(db, referencefile_mod.referencefile_mod_id)
    else:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail="You are not signed in. Please sign in to delete a file.")

    if file_class == 'main' and file_publication_status == 'final' and pdf_type == 'pdf':
        cleanup_wft_tet_tags_for_deleted_main_pdf(db, reference_id, all_mods,
                                                  MOD_ACCESS_ABBR[mod_access])


def cleanup_wft_tet_tags_for_deleted_main_pdf(db: Session, reference_id, all_mods, access_level, change_file_status=False):

    mods = set()
    if access_level != 'all_access':
        mods.add(access_level)
    elif len(all_mods) > 0 and 'PMC' not in all_mods:
        mods = all_mods
    else:
        sql_query = text("""
        SELECT m.abbreviation
        FROM mod_corpus_association mca
        JOIN mod m ON mca.mod_id = m.mod_id
        WHERE mca.reference_id = :reference_id
        AND mca.corpus = TRUE
        """)
        rows = db.execute(sql_query, {'reference_id': reference_id}).fetchall()
        mods.update(row[0] for row in rows)
    for mod_abbreviation in mods:
        reset_workflow_tags_after_deleting_main_pdf(db, str(reference_id), mod_abbreviation, change_file_status)
        if change_file_status is False:
            delete_non_manual_tets(db, str(reference_id), mod_abbreviation)
            has_manual_tags = has_manual_tet(db, str(reference_id), mod_abbreviation)
            if has_manual_tags:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                    detail="Curated topic and entity tags or automated tags generated from your MOD are associated with this reference. Please check with the curator who added these tags.")


def merge_referencefiles(db: Session,
                         curie_or_reference_id: str,
                         losing_referencefile_id: int,
                         winning_referencefile_id: int):
    """
    :param db:
    :param curie_or_reference_id:
    :param losing_referencefile_id:
    :param winning_referencefile_id:
    :return:

    Transfer referencefile_mods from losing referencefile to winning referencefile, unless already has a referencefile_mod
    with that mod.
    Then delete losing_referencefile.
    Then attach winning referencefile to reference, if it's not already attached to it.
    """
    logger.info("Merging referencefiles")
    reference = get_reference(db=db, curie_or_reference_id=curie_or_reference_id, load_referencefiles=True)

    # Lookup both referencefiles
    losing_referencefile = read_referencefile_db_obj(db, losing_referencefile_id)
    winning_referencefile = read_referencefile_db_obj(db, winning_referencefile_id)

    winning_mod_set = {referencefile_mod.mod.abbreviation if referencefile_mod.mod is not None else None
                       for referencefile_mod in winning_referencefile.referencefile_mods}

    for referencefile_mod in losing_referencefile.referencefile_mods:
        mod_abbreviation = referencefile_mod.mod.abbreviation if referencefile_mod.mod is not None else None
        if mod_abbreviation not in winning_mod_set:
            referencefile_mod.referencefile_id = winning_referencefile.referencefile_id
            db.add(referencefile_mod)
    db.commit()
    # call destroy on losing_referencefile or something else because it needs mod_access, and that will remove from s3 ?
    db.delete(losing_referencefile)

    if winning_referencefile.reference_id != reference.reference_id:
        patch(db, winning_referencefile_id, ReferencefileSchemaUpdate(reference_curie=reference.curie).model_dump(
            exclude_unset=True))

    db.commit()


def file_paths_in_dir(directory):
    for dirpath, _, filenames in os.walk(directory):
        for f in filenames:
            file_path = os.path.abspath(os.path.join(dirpath, f))
            if os.path.isfile(file_path):
                yield file_path


def check_if_paper_in_corpus(db, reference_curie, mod_abbr):
    query = text("""
        SELECT mca.corpus
        FROM mod_corpus_association mca
        JOIN reference r ON mca.reference_id = r.reference_id
        JOIN mod m ON mca.mod_id = m.mod_id
        WHERE r.curie = :reference_curie
          AND m.abbreviation = :mod_abbr
    """)
    row = db.execute(query, {"reference_curie": reference_curie, "mod_abbr": mod_abbr}).fetchone()
    return bool(row and row[0])


def file_upload(db: Session, metadata: dict, file: UploadFile, upload_if_already_converted: bool = False):  # pragma: no cover
    metadata["reference_curie"] = normalize_reference_curie(db, metadata["reference_curie"])
    if metadata["mod_abbreviation"]:
        inCorpus = check_if_paper_in_corpus(db, metadata["reference_curie"], metadata["mod_abbreviation"])
        if not inCorpus:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail=f"This paper ({metadata['reference_curie']}) is not in {metadata['mod_abbreviation']}.")
        job_type = is_file_upload_blocked(db, metadata["reference_curie"], metadata["mod_abbreviation"])
        if job_type:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail=f"The {job_type} for reference {metadata['reference_curie']} is currently in progress. Please wait until the {job_type} process is complete before uploading any files for this paper.")

    if (
        metadata["file_class"] == 'main'
        and metadata["file_publication_status"] == 'final'
        and metadata["file_extension"] == 'pdf'
        and metadata.get("pdf_type") in (None, '')
    ):
        metadata["pdf_type"] = 'pdf'

    if metadata['file_class'] == 'main' and metadata['file_publication_status'] == 'final' and metadata['file_extension'] == 'pdf':
        if 'pdf_type' not in metadata or metadata["pdf_type"] == '':
            metadata["pdf_type"] = 'pdf'

    if not upload_if_already_converted and metadata["mod_abbreviation"] and metadata["pdf_type"] == 'pdf' and metadata['file_class'] == 'main' and metadata['file_publication_status'] == 'final':
        workflow_tag_atp_id = get_current_workflow_status(db,
                                                          metadata["reference_curie"],
                                                          text_conversion_process_atp_id,
                                                          metadata["mod_abbreviation"])
        if workflow_tag_atp_id == "ATP:0000163":  # file converted to text
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail="File already converted to text, use UI if you really need to replace the file.")

    created_referencefiles = []
    if metadata["file_extension"] in ["tgz", "tar.gz"]:
        temp_dir = tempfile.mkdtemp()
        file_tar = tarfile.open(fileobj=file.file)
        file_tar.extractall(temp_dir)
        error_message = ""
        for file_path in file_paths_in_dir(temp_dir):
            file_name = os.path.basename(file_path)
            single_file_metadata = copy.deepcopy(metadata)
            if file_name.lower().endswith(".tar.gz"):
                single_file_metadata["display_name"] = ".".join(file_name.split(".")[0:-2])
                single_file_metadata["file_extension"] = ".".join(file_name.split(".")[-2:])
            else:
                single_file_metadata["display_name"] = ".".join(file_name.split(".")[0:-1])
                single_file_metadata["file_extension"] = file_name.split(".")[-1]
            try:
                with open(file_path, "rb") as f_in:
                    created_referencefiles.append(
                        file_upload_single(db, single_file_metadata, UploadFile(filename=file_name, file=f_in))
                    )
            except HTTPException as e:
                error_message += single_file_metadata["display_name"] + "." + single_file_metadata[
                    "file_extension"] + " upload failed: " + e.detail
                if not error_message.endswith("."):
                    error_message += "."
                error_message += " "
        shutil.rmtree(temp_dir, ignore_errors=True)
        if error_message:
            error_message += "Any other files were uploaded"
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=error_message)
    else:
        created_referencefiles.append(file_upload_single(db, metadata, file))
    mod_abbreviation = metadata["mod_abbreviation"] if "mod_abbreviation" in metadata else None
    if _suppress_post_upload_workflow:
        # Auxiliary upload (e.g. figure-metadata sidecar): persist the file but
        # do not move the reference's workflow status or prune its main PDFs.
        # transition_WFT_for_uploaded_file is normally the committing step, so
        # commit explicitly to guarantee the new referencefile is persisted.
        db.commit()
    else:
        cleanup_old_pdf_file(db, metadata["reference_curie"], mod_abbreviation)
        transition_WFT_for_uploaded_file(db, metadata["reference_curie"], mod_abbreviation,
                                         metadata["file_class"], metadata["pdf_type"],
                                         metadata["file_publication_status"])
    return created_referencefiles


def transition_WFT_for_uploaded_file(db, reference_curie, mod_abbreviation, file_class, pdf_type, file_publication_status, change_file_status=False):
    logger.info("Transition WFT for uploaded file")
    if file_class == 'main' and pdf_type == 'pdf' and file_publication_status == 'final':
        wft_tag_atp_id = file_uploaded_tag_atp_id
    else:
        wft_tag_atp_id = file_upload_in_progress_tag_atp_id

    ref = get_reference(db=db, curie_or_reference_id=reference_curie)

    if mod_abbreviation is None:
        rows = db.execute(text(f"SELECT m.abbreviation "
                               f"FROM mod m, mod_corpus_association mca "
                               f"WHERE m.mod_id = mca.mod_id "
                               f"AND mca.reference_id = {ref.reference_id} "
                               f"AND mca.corpus is True")).mappings().fetchall()
        mods = {x['abbreviation'] for x in rows}
    else:
        mods = {mod_abbreviation}
    for mod in mods:
        try:
            curr_tag_atp_id = get_current_workflow_status(db, reference_curie,
                                                          file_upload_process_atp_id, mod)
            if change_file_status is False:
                if curr_tag_atp_id is None:
                    data = WorkflowTagSchemaPost(
                        workflow_tag_id=wft_tag_atp_id,
                        mod_abbreviation=mod,
                        reference_curie=reference_curie
                    )
                    create_wft(db, data)
                elif (curr_tag_atp_id != wft_tag_atp_id and curr_tag_atp_id != file_uploaded_tag_atp_id):
                    transition_to_workflow_status(db, reference_curie, mod, wft_tag_atp_id)
            else:
                # this should not happen, but just in case
                if curr_tag_atp_id and curr_tag_atp_id == wft_tag_atp_id:
                    continue
                if curr_tag_atp_id and curr_tag_atp_id == file_uploaded_tag_atp_id:
                    cleanup_wft_tet_tags_for_deleted_main_pdf(db, ref.reference_id, {mod}, mod, change_file_status)
                transition_to_workflow_status(db, reference_curie, mod, wft_tag_atp_id)
        except Exception as e:
            db.rollback()
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail=f"Transitioning file_upload WFT for reference_curie = {reference_curie}, mod={mod} failed. error={e}")
    db.commit()


def cleanup_old_pdf_file(db: Session, ref_curie: str, mod_abbreviation):  # pragma: no cover
    ref = db.query(ReferenceModel).filter_by(curie=ref_curie).one_or_none()
    if ref:
        reffiles = db.query(ReferencefileModel).filter_by(
            reference_id=ref.reference_id, file_class='main', pdf_type='pdf', file_publication_status='final').order_by(
                ReferencefileModel.file_publication_status, ReferencefileModel.date_updated.desc()).all()

        if len(reffiles) >= 2:
            mod_ids_with_final = {mod.mod_id for reffile in reffiles for mod in reffile.referencefile_mods if
                                  reffile.file_publication_status == 'final'}
            final_files_to_keep = set()
            temp_files_to_delete = []
            final_files_with_none = None

            for reffile in reffiles:
                if reffile.file_publication_status == 'temp':
                    if None in mod_ids_with_final or all(mod.mod_id in mod_ids_with_final for mod in reffile.referencefile_mods):
                        temp_files_to_delete.append(reffile.referencefile_id)
                elif reffile.file_publication_status == 'final':
                    if any(mod.mod_id is None for mod in reffile.referencefile_mods):
                        if final_files_with_none is not None:
                            temp_files_to_delete.append(reffile.referencefile_id)
                        else:
                            final_files_with_none = reffile.referencefile_id
                    for mod in reffile.referencefile_mods:
                        if mod.mod_id is not None:
                            if mod.mod_id in final_files_to_keep:
                                temp_files_to_delete.append(reffile.referencefile_id)
                            else:
                                final_files_to_keep.add(mod.mod_id)

            # Delete the temp files and older final files
            for file_id in temp_files_to_delete:
                destroy(db, file_id, [access for access, mod_abbr in MOD_ACCESS_ABBR.items() if
                                      mod_abbr == mod_abbreviation][0])


def create_metadata(db: Session, request: ReferencefileSchemaPost):
    request_dict = request.model_dump()

    if "created_by" in request_dict and request_dict["created_by"] is not None:
        request_dict["created_by"] = map_to_user_id(request_dict["created_by"], db)
    if "updated_by" in request_dict and request_dict["updated_by"] is not None:
        request_dict["updated_by"] = map_to_user_id(request_dict["updated_by"], db)

    ref_obj = db.query(ReferenceModel).filter(ReferenceModel.curie == request.reference_curie).one_or_none()
    if ref_obj is None:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"Reference with curie {request.reference_curie} does not exist")
    del request_dict["reference_curie"]
    request_dict["reference_id"] = ref_obj.reference_id
    mod_abbreviation = request_dict["mod_abbreviation"]
    if mod_abbreviation is not None:
        mod = db.query(ModModel).filter(ModModel.abbreviation == mod_abbreviation).one_or_none()
        if mod is None:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail=f"Mod with abbreviation {request.mod_abbreviation} does not exist")
    del request_dict["mod_abbreviation"]

    # if it is a main PDF, set the pdf_type = 'pdf' if it is NULL
    if (
        request_dict["file_class"] == 'main'
        and request_dict["file_publication_status"] == 'final'
        and request_dict["file_extension"] == 'pdf'
        and request_dict.get("pdf_type") in (None, '')
    ):
        request_dict["pdf_type"] = 'pdf'

    new_ref_file_obj = ReferencefileModel(**request_dict)
    db.add(new_ref_file_obj)
    db.commit()
    create_mod_connection(db, ReferencefileModSchemaPost(referencefile_id=new_ref_file_obj.referencefile_id,
                                                         mod_abbreviation=mod_abbreviation))
    return new_ref_file_obj.referencefile_id


def find_first_available_display_name(display_name: str, file_extension: str, reference_curie: str, db: Session):
    original_name = display_name
    for counter in count(start=1, step=1):
        ref_file_same_name = db.query(ReferencefileModel).filter(
            and_(
                ReferencefileModel.display_name == display_name,
                ReferencefileModel.file_extension == file_extension,
                ReferencefileModel.reference.has(ReferenceModel.curie == reference_curie)
            )
        ).one_or_none()
        if ref_file_same_name is None:
            break
        display_name = f"{original_name}_{counter}"
    return display_name


def file_upload_single(db: Session, metadata: dict, file: UploadFile):  # pragma: no cover
    mod_abbreviation = metadata["mod_abbreviation"] if "mod_abbreviation" in metadata else None
    file.file.seek(0)
    md5sum_hash = hashlib.md5()
    for byte_block in iter(lambda: file.file.read(4096), b""):
        md5sum_hash.update(byte_block)
    md5sum = md5sum_hash.hexdigest()
    folder = get_s3_folder_from_md5sum(md5sum)
    referencefile_instance: ReferencefileModel = db.query(ReferencefileModel).filter(
        and_(
            ReferencefileModel.md5sum == md5sum,
            ReferencefileModel.reference.has(ReferenceModel.curie == metadata["reference_curie"])
        )
    ).one_or_none()
    if referencefile_instance is not None:
        # the file already exists, and it's already associated with the provided reference, but the metadata in the
        # request may be incompatible with the one in the db. The metadata in the db will not be modified and a new
        # connection between the file and the mod will be created. See below for special cases for WB
        if mod_abbreviation == "WB":
            # if a final file is uploaded by WB and the same file is in the system as temp, then set it to final
            if metadata["file_publication_status"] == "final" and referencefile_instance.file_publication_status == "temp":
                referencefile_instance.file_publication_status = "final"  # type: ignore
            # If WB uploads a temp and the same file is already present but not for WB, then set the status to temp
            elif "WB" not in {referencefile_mod.mod.abbreviation for referencefile_mod in
                              referencefile_instance.referencefile_mods if referencefile_mod.mod is not None} and \
                    metadata["file_publication_status"] == "temp" and referencefile_instance.file_publication_status == "final":
                referencefile_instance.file_publication_status = "temp"  # type: ignore
            db.commit()
        if all(referencefile_mod.mod.abbreviation != mod_abbreviation for referencefile_mod in
               referencefile_instance.referencefile_mods if referencefile_mod.mod is not None):
            create_mod_connection(db, ReferencefileModSchemaPost(referencefile_id=referencefile_instance.referencefile_id,
                                                                 mod_abbreviation=mod_abbreviation))
    else:
        # 2 possible cases here: i) an entry with the same md5sum does not exist; ii) same md5sum exists, but it's
        # associated with a different curie (same file content for different files or same files). In both cases we need
        # to create a new referencefile and associate it with the specified ref and mod

        # check if a different version of the same file (same display_name and file_extension) is already associated
        # with the same reference. Add _num to the display name to upload different versions of the same file
        metadata["display_name"] = find_first_available_display_name(
            display_name=metadata["display_name"], file_extension=metadata["file_extension"],
            reference_curie=metadata["reference_curie"], db=db)
        create_request = ReferencefileSchemaPost(md5sum=md5sum, **metadata)
        new_referencefile_id = create_metadata(db, create_request)
        referencefile_instance = db.query(ReferencefileModel).filter_by(
            referencefile_id=new_referencefile_id
        ).one()
        # check if md5sum is the only one in the db before uploading to s3
        ref_file_by_md5sum_count = db.query(ReferencefileModel).filter(ReferencefileModel.md5sum == md5sum).count()
        if ref_file_by_md5sum_count == 1:
            file.file.seek(0)
            temp_file_name = metadata["display_name"] + "." + metadata["file_extension"] + ".gz"
            with gzip.open(temp_file_name, 'wb') as f_out:
                shutil.copyfileobj(file.file, f_out)
            client = boto3.client('s3')
            env_state = os.environ.get("ENV_STATE", "")
            extra_args = {'StorageClass': 'GLACIER_IR'} if env_state == "prod" else {'StorageClass': 'STANDARD'}
            with open(temp_file_name, 'rb') as gzipped_file:
                upload_file_to_bucket(s3_client=client, file_obj=gzipped_file, bucket="agr-literature", folder=folder,
                                      object_name=md5sum + ".gz", ExtraArgs=extra_args)
            os.remove(temp_file_name)
    return referencefile_instance


def download_file(db: Session, referencefile_id: int, mod_access: ModAccess,  # pragma: no cover
                  use_in_api: bool = True):  # pragma: no cover
    referencefile = read_referencefile_db_obj(db, referencefile_id)

    user_permission = False
    if referencefile.reference.copyright_license:
        user_permission = referencefile.reference.copyright_license.open_access

    if user_permission is False:
        if mod_access != ModAccess.NO_ACCESS:
            if mod_access == ModAccess.ALL_ACCESS or any(
                    ref_file_mod.mod.abbreviation == MOD_ACCESS_ABBR[mod_access] if ref_file_mod.mod is not None else
                    True for ref_file_mod in referencefile.referencefile_mods):
                user_permission = True

    if user_permission is True:
        md5sum = referencefile.md5sum
        display_name = referencefile.display_name + "." + referencefile.file_extension
        folder = get_s3_folder_from_md5sum(md5sum)
        object_name = folder + "/" + md5sum + ".gz"
        download_file_from_s3(md5sum + ".gz", bucketname="agr-literature", s3_file_location=object_name)
        with gzip.open(md5sum + ".gz", 'rb') as f_in, open(display_name, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
        os.remove(md5sum + ".gz")
        if use_in_api:
            return FileResponse(path=display_name, filename=display_name, media_type="application/octet-stream",
                                background=BackgroundTask(cleanup, display_name))
        else:
            with open(display_name, 'rb') as file:
                file_content = file.read()
            os.remove(display_name)
            return file_content

    raise HTTPException(status_code=status.HTTP_403_FORBIDDEN,
                        detail="The current user does not have permissions to get the requested file url. "
                               "The associated paper is not available for free access.")


def cleanup(file_path):
    os.remove(file_path)


def download_additional_files_tarball(db: Session, reference_id, mod_access: ModAccess):
    ref_curie = db.query(ReferenceModel.curie).filter(ReferenceModel.reference_id == reference_id).one_or_none()
    if not ref_curie:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="No reference found for the specified reference_id")
    ref_curie = ref_curie.curie
    all_referencefile_supp = db.query(ReferencefileModel).options(
        subqueryload(
            ReferencefileModel.referencefile_mods)
    ).filter(
        and_(
            ReferencefileModel.reference_id == reference_id,
            ReferencefileModel.file_class != "main",
            ReferencefileModel.file_class != "correspondence",
            or_(
                ReferencefileModel.reference.has(ReferenceModel.copyright_license.has(
                    CopyrightLicenseModel.open_access == True)), # noqa
                ReferencefileModel.referencefile_mods.any(
                    or_(
                        ReferencefileModAssociationModel.mod == None, # noqa
                        ReferencefileModAssociationModel.mod.has(
                            ModModel.abbreviation == MOD_ACCESS_ABBR[mod_access])
                    )
                )
            )
        )
    ).all()

    tar_file_path = ref_curie.replace(":", "_") + "_additional_files.tar.gz"
    os.makedirs("tarball_tmp", exist_ok=True)
    with tarfile.open(tar_file_path, "w:gz") as tar:
        for referencefile in all_referencefile_supp:
            md5sum = referencefile.md5sum
            folder = get_s3_folder_from_md5sum(md5sum)
            object_name = folder + "/" + md5sum + ".gz"
            tmp_file_gz_path = "tarball_tmp/" + referencefile.display_name + ".gz"
            tmp_file_name = referencefile.display_name + "." + referencefile.file_extension
            tmp_file_path = "tarball_tmp/" + tmp_file_name
            download_file_from_s3(tmp_file_gz_path, "agr-literature", object_name)
            with gzip.open(tmp_file_gz_path, 'rb') as f_in, open(tmp_file_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
            tar.add(tmp_file_path, arcname=tmp_file_name)
            os.remove(tmp_file_path)
    shutil.rmtree("tarball_tmp")
    return FileResponse(path=tar_file_path, filename=tar_file_path, media_type="application/gzip",
                        background=BackgroundTask(cleanup, tar_file_path))
