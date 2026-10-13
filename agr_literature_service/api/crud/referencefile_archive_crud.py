"""
Archive upload: attach a MOD's existing main PDFs to their ABC references
WITHOUT sending the papers to text conversion (SCRUM-6633).

The file is named after the MOD's own ID for the paper, i.e. the local part of
the reference's MOD cross-reference: ``FBrf0111489.pdf`` -> ``FB:FBrf0111489``,
``WBPaper00031514.pdf`` -> ``WB:WBPaper00031514``, ``XB-ART-59369.pdf`` ->
``Xenbase:XB-ART-59369``. The resulting curie must match the MOD's reference
pattern in ``utils/yml/reference.yml``.

A normal upload (``file_upload``) moves the MOD's file-upload workflow status
with ``transition_WFT_for_uploaded_file``, whose transition actions queue text
conversion (ATP:0000162), and prunes all but the newest main PDF with
``cleanup_old_pdf_file``. An archive upload skips both, then sets the
file-upload status to "files uploaded" (ATP:0000134) directly: created when
the paper has no file-upload tag yet, otherwise overwritten in place. Neither
path runs transition actions, and the text-conversion tags are never touched.

Per file the result status is one of:

- ``bad_name``: the filename is not ``<MOD ID>.pdf``;
- ``no_xref``: no live cross-reference for that ID;
- ``not_in_corpus``: the reference is not in the MOD's corpus;
- ``already_loaded``: the reference already has this exact file (same md5) as
  a main PDF of the MOD; its status is still set to "files uploaded", so
  re-sending a file finishes an interrupted upload;
- ``has_other_mod_pdf``: the reference already has a different main PDF of
  the MOD (``include_existing`` uploads alongside it instead; nothing is ever
  deleted);
- ``would_upload`` (``dry_run``) / ``uploaded``.
"""
import hashlib
import re
from os import path
from typing import Any, Dict, Optional, Set

from fastapi import HTTPException, UploadFile, status
from sqlalchemy.orm import Session

from agr_literature_service.api.crud.referencefile_crud import (
    check_if_paper_in_corpus,
    file_upload,
    file_upload_process_atp_id,
    file_uploaded_tag_atp_id,
)
from agr_literature_service.api.crud.utils.patterns_check import get_patterns
from agr_literature_service.api.crud.workflow_tag_crud import (
    _get_current_workflow_tag_db_obj,
    create as create_wft,
)
from agr_literature_service.api.models import (
    CrossReferenceModel,
    ModModel,
    ReferenceModel,
    ReferencefileModAssociationModel,
    ReferencefileModel,
)
from agr_literature_service.api.schemas.workflow_tag_schemas import WorkflowTagSchemaPost


def xref_curie_prefix(mod_abbreviation: str) -> str:
    return "Xenbase" if mod_abbreviation == "XB" else mod_abbreviation


def archive_xref_curie(mod_abbreviation: str, filename: str) -> Optional[str]:
    """``FBrf0111489.pdf`` -> ``FB:FBrf0111489`` for FB; None when the name is
    not ``<ID>.pdf`` or the curie does not match the MOD's reference pattern."""
    stem, extension = path.splitext(path.basename(filename))
    if extension.lower() != ".pdf" or not stem:
        return None
    prefix = xref_curie_prefix(mod_abbreviation)
    pattern = get_patterns()["reference"].get(prefix)
    curie = f"{prefix}:{stem}"
    if pattern is None or not re.match(pattern, curie):
        return None
    return curie


def find_reference(db: Session, xref_curie: str) -> Optional[ReferenceModel]:
    """The reference carrying the live cross-reference ``xref_curie``."""
    return (
        db.query(ReferenceModel)
        .join(CrossReferenceModel, CrossReferenceModel.reference_id == ReferenceModel.reference_id)
        .filter(CrossReferenceModel.curie == xref_curie,
                CrossReferenceModel.is_obsolete.is_(False))
        .first()
    )


def mod_main_pdf_md5s(db: Session, reference_id: int, mod_abbreviation: str) -> Set[str]:
    """md5sums of the reference's main / final / pdf files linked to the MOD."""
    rows = (
        db.query(ReferencefileModel.md5sum)
        .join(ReferencefileModAssociationModel,
              ReferencefileModAssociationModel.referencefile_id == ReferencefileModel.referencefile_id)
        .join(ModModel, ModModel.mod_id == ReferencefileModAssociationModel.mod_id)
        .filter(ReferencefileModel.reference_id == reference_id,
                ReferencefileModel.file_class == "main",
                ReferencefileModel.file_publication_status == "final",
                ReferencefileModel.pdf_type == "pdf",
                ModModel.abbreviation == mod_abbreviation)
        .all()
    )
    return {row.md5sum for row in rows}


def set_file_uploaded_status(db: Session, reference_curie: str, mod_abbreviation: str) -> Optional[str]:
    """Set the reference's file-upload workflow status for the MOD to "files
    uploaded" WITHOUT running transition actions, so nothing queues text
    conversion.

    Returns the change made: ``"created"``, ``"<old ATP> -> ATP:0000134"``, or
    None when the status already was "files uploaded"."""
    current = _get_current_workflow_tag_db_obj(db, reference_curie, file_upload_process_atp_id, mod_abbreviation)
    if current is None:
        create_wft(db, WorkflowTagSchemaPost(workflow_tag_id=file_uploaded_tag_atp_id,
                                             mod_abbreviation=mod_abbreviation,
                                             reference_curie=reference_curie))
        return "created"
    if current.workflow_tag_id == file_uploaded_tag_atp_id:
        return None
    old_tag = current.workflow_tag_id
    current.workflow_tag_id = file_uploaded_tag_atp_id
    db.commit()
    return f"{old_tag} -> {file_uploaded_tag_atp_id}"


def file_md5(file: UploadFile) -> str:
    file.file.seek(0)
    md5sum_hash = hashlib.md5()
    for byte_block in iter(lambda: file.file.read(1 << 20), b""):
        md5sum_hash.update(byte_block)
    file.file.seek(0)
    return md5sum_hash.hexdigest()


def archive_upload(db: Session, mod_abbreviation: str, file: UploadFile,
                   include_existing: bool = False, dry_run: bool = False) -> Dict[str, Any]:
    if db.query(ModModel).filter(ModModel.abbreviation == mod_abbreviation).one_or_none() is None:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                            detail=f"Unknown MOD {mod_abbreviation}")
    filename = path.basename(file.filename or "")
    result: Dict[str, Any] = {"file_name": filename, "status": None, "xref_curie": None,
                              "reference_curie": None, "workflow_change": None}

    xref_curie = archive_xref_curie(mod_abbreviation, filename)
    if xref_curie is None:
        result["status"] = "bad_name"
        return result
    result["xref_curie"] = xref_curie
    reference = find_reference(db, xref_curie)
    if reference is None:
        result["status"] = "no_xref"
        return result
    reference_curie = reference.curie
    result["reference_curie"] = reference_curie
    if not check_if_paper_in_corpus(db, reference_curie, mod_abbreviation):
        result["status"] = "not_in_corpus"
        return result

    existing = mod_main_pdf_md5s(db, reference.reference_id, mod_abbreviation)
    if file_md5(file) in existing:
        result["status"] = "already_loaded"
        if not dry_run:
            result["workflow_change"] = set_file_uploaded_status(db, reference_curie, mod_abbreviation)
        return result
    if existing and not include_existing:
        result["status"] = "has_other_mod_pdf"
        return result
    if dry_run:
        result["status"] = "would_upload"
        return result

    stem = path.splitext(filename)[0]
    metadata = {
        "reference_curie": reference_curie,
        "display_name": stem,
        "file_class": "main",
        "file_publication_status": "final",
        "file_extension": "pdf",
        "pdf_type": "pdf",
        "is_annotation": False,
        "mod_abbreviation": mod_abbreviation,
    }
    file_upload(db=db, metadata=metadata, file=UploadFile(file=file.file, filename=f"{stem}.pdf"),
                upload_if_already_converted=True, skip_post_upload_actions=True)
    result["status"] = "uploaded"
    result["workflow_change"] = set_file_uploaded_status(db, reference_curie, mod_abbreviation)
    return result
