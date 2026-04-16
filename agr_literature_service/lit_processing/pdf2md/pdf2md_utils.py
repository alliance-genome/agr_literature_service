"""
Utility functions for PDF to Markdown conversion.

This module provides helper functions for processing PDFs for individual papers,
including reference lookup by curie and PDF file retrieval.
"""
import logging
from io import BytesIO
from typing import Dict, List, Literal, Optional, Tuple

from fastapi import HTTPException, UploadFile
from sqlalchemy.orm import Session

from agr_literature_service.api.crud.referencefile_crud import download_file, file_upload
from agr_literature_service.api.crud.reference_utils import (
    normalize_reference_curie,
    get_reference,
)
from agr_literature_service.api.models import (
    ReferencefileModel,
    ReferenceModel,
)
from agr_cognito_py import ModAccess

from .pdf2md import (
    get_pdfx_token,
    submit_pdf_to_pdfx,
    poll_pdfx_status,
    download_pdfx_result,
    EXTRACTION_METHODS,
)


logger = logging.getLogger(__name__)

# Valid pdf_type values
PdfType = Literal["main", "supplement", "both"]


def resolve_curie_to_reference(db: Session, curie: str) -> Optional[ReferenceModel]:
    """
    Resolve a curie to a ReferenceModel.

    The curie can be:
    - A reference curie (AGRKB ID, e.g., "AGRKB:101000000000001")
    - A cross-reference curie (e.g., "PMID:12345", "PMC:PMC12345", "WB:WBPaper00000001")

    Uses the existing normalize_reference_curie and get_reference functions
    from agr_literature_service.api.crud.reference_utils.

    Args:
        db: Database session.
        curie: The curie to resolve.

    Returns:
        ReferenceModel if found, None otherwise.
    """
    try:
        # Normalize curie to AGRKB format if it's a cross-reference curie
        normalized_curie = normalize_reference_curie(db, curie)
        logger.debug(f"Normalized curie: {curie} -> {normalized_curie}")

        # Get the reference using the normalized curie
        reference = get_reference(db, normalized_curie)
        return reference

    except HTTPException as e:
        logger.warning(f"Could not resolve curie to reference: {curie} - {e.detail}")
        return None


def get_pdf_files_for_reference(
    db: Session,
    reference_id: int,
    pdf_type: PdfType = "main"
) -> List[ReferencefileModel]:
    """
    Get PDF files for a reference based on pdf_type.

    Args:
        db: Database session.
        reference_id: The reference ID to get PDFs for.
        pdf_type: Type of PDFs to retrieve:
            - "main": Main PDF only (file_class='main')
            - "supplement": Supplemental PDFs only (file_class='supplement')
            - "both": Both main and supplemental PDFs

    Returns:
        List of ReferencefileModel objects matching the criteria.
    """
    file_classes = []
    if pdf_type == "main":
        file_classes = ["main"]
    elif pdf_type == "supplement":
        file_classes = ["supplement"]
    elif pdf_type == "both":
        file_classes = ["main", "supplement"]

    query = db.query(ReferencefileModel).filter(
        ReferencefileModel.reference_id == reference_id,
        ReferencefileModel.file_class.in_(file_classes),
        ReferencefileModel.file_extension == "pdf",
        ReferencefileModel.file_publication_status == "final"
    )

    # Order by file_class to process main PDFs first
    pdf_files = query.order_by(
        ReferencefileModel.file_class.desc()  # 'supplement' > 'main' alphabetically, so desc puts main first
    ).all()

    return pdf_files


def process_pdf_for_reference(
    db: Session,
    curie: str,
    pdf_type: PdfType = "main",
    methods_to_extract: Optional[List[str]] = None,
    token: Optional[str] = None
) -> Dict:
    """
    Process PDF(s) for a single reference and convert to Markdown.

    This function:
    1. Resolves the curie to a reference
    2. Finds the appropriate PDF file(s) based on pdf_type
    3. Submits each PDF to PDFX service for conversion
    4. Downloads and stores the resulting Markdown files

    Args:
        db: Database session.
        curie: Reference curie (AGRKB ID) or cross-reference curie
               (e.g., PMID:12345, PMC:PMC12345, WB:WBPaper00000001).
        pdf_type: Type of PDFs to process:
            - "main": Main PDF only (file_class='main', file_extension='pdf',
                      file_publication_status='final')
            - "supplement": Supplemental PDFs only (file_class='supplement',
                           file_extension='pdf', file_publication_status='final')
            - "both": Both main and supplemental PDFs
        methods_to_extract: List of extraction methods to use.
                           Default: ["grobid", "docling", "marker", "merged"]
        token: Optional pre-fetched PDFX token. If not provided, will be fetched.

    Returns:
        Dict with processing results:
        {
            "success": bool,
            "reference_curie": str,
            "input_curie": str,
            "pdfs_processed": int,
            "pdfs_succeeded": int,
            "pdfs_failed": int,
            "details": [
                {
                    "referencefile_id": int,
                    "display_name": str,
                    "file_class": str,
                    "success": bool,
                    "methods_uploaded": List[str],
                    "error": Optional[str]
                },
                ...
            ],
            "error": Optional[str]  # Only if complete failure
        }
    """
    if methods_to_extract is None:
        methods_to_extract = list(EXTRACTION_METHODS.keys())

    result = {
        "success": False,
        "reference_curie": None,
        "input_curie": curie,
        "pdfs_processed": 0,
        "pdfs_succeeded": 0,
        "pdfs_failed": 0,
        "details": [],
        "error": None
    }

    # Resolve curie to reference
    reference = resolve_curie_to_reference(db, curie)
    if not reference:
        result["error"] = f"Could not resolve curie to reference: {curie}"
        logger.error(result["error"])
        return result

    result["reference_curie"] = reference.curie
    reference_curie = reference.curie

    # Get PDF files
    pdf_files = get_pdf_files_for_reference(db, reference.reference_id, pdf_type)
    if not pdf_files:
        result["error"] = f"No PDF files found for {reference_curie} with pdf_type='{pdf_type}'"
        logger.warning(result["error"])
        return result

    logger.info(f"Found {len(pdf_files)} PDF file(s) for {reference_curie} (pdf_type={pdf_type})")

    # Get token if not provided
    if token is None:
        try:
            token = get_pdfx_token()
        except Exception as e:
            result["error"] = f"Failed to obtain PDFX token: {e}"
            logger.error(result["error"])
            return result

    # Process each PDF file
    for pdf_file in pdf_files:
        result["pdfs_processed"] += 1
        pdf_detail = {
            "referencefile_id": pdf_file.referencefile_id,
            "display_name": pdf_file.display_name,
            "file_class": pdf_file.file_class,
            "success": False,
            "methods_uploaded": [],
            "error": None
        }

        try:
            success, methods_uploaded, error_msg = _process_single_pdf_file(
                db=db,
                pdf_file=pdf_file,
                reference_curie=reference_curie,
                token=token,
                methods_to_extract=methods_to_extract
            )

            pdf_detail["success"] = success
            pdf_detail["methods_uploaded"] = methods_uploaded
            pdf_detail["error"] = error_msg

            if success:
                result["pdfs_succeeded"] += 1
            else:
                result["pdfs_failed"] += 1

        except Exception as e:
            pdf_detail["error"] = str(e)
            result["pdfs_failed"] += 1
            logger.error(f"Error processing PDF {pdf_file.display_name}: {e}")

        result["details"].append(pdf_detail)

    # Overall success if at least one PDF was successfully processed
    result["success"] = result["pdfs_succeeded"] > 0

    return result


def _process_single_pdf_file(
    db: Session,
    pdf_file: ReferencefileModel,
    reference_curie: str,
    token: str,
    methods_to_extract: List[str]
) -> Tuple[bool, List[str], Optional[str]]:
    """
    Process a single PDF file through PDFX.

    Args:
        db: Database session.
        pdf_file: ReferencefileModel object for the PDF.
        reference_curie: The reference curie for logging and metadata.
        token: PDFX bearer token.
        methods_to_extract: List of extraction methods to use.

    Returns:
        Tuple of (success: bool, methods_uploaded: List[str], error_message: Optional[str]).
    """
    display_name = pdf_file.display_name
    file_class = pdf_file.file_class

    # Get MOD abbreviation from referencefile_mods
    mod_abbreviation = None
    for ref_file_mod in pdf_file.referencefile_mods:
        if ref_file_mod.mod:
            mod_abbreviation = ref_file_mod.mod.abbreviation
            break

    logger.info(f"Processing {file_class} PDF: {display_name} for {reference_curie}")

    # Download the PDF content
    file_content = download_file(
        db=db,
        referencefile_id=pdf_file.referencefile_id,
        mod_access=ModAccess.ALL_ACCESS,
        use_in_api=False
    )

    if not file_content:
        return False, [], "Failed to download PDF content"

    # Determine which methods to request
    request_methods = [m for m in methods_to_extract if m != "merged"]
    include_merge = "merged" in methods_to_extract

    # Submit to PDFX
    process_id = submit_pdf_to_pdfx(
        file_content=file_content,
        token=token,
        methods=",".join(request_methods) if request_methods else "grobid,docling,marker",
        merge=include_merge,
        reference_curie=reference_curie,
        mod_abbreviation=mod_abbreviation
    )

    # Poll for completion
    poll_pdfx_status(process_id, token)

    # Download and store each method's output
    successful_methods = []
    for method in methods_to_extract:
        try:
            markdown_content = download_pdfx_result(process_id, method, token)

            if not markdown_content or len(markdown_content) < 10:
                logger.warning(f"Empty or minimal content for {method} on {reference_curie}")
                continue

            # Determine file class for output based on input file class
            if file_class == "main":
                output_file_class = EXTRACTION_METHODS[method]
            else:
                # For supplement files, use a different naming convention
                output_file_class = f"converted_{method}_supplement"

            output_display_name = f"{display_name}_{method}"

            metadata = {
                "reference_curie": reference_curie,
                "display_name": output_display_name,
                "file_class": output_file_class,
                "file_publication_status": "final",
                "file_extension": "md",
                "pdf_type": None,
                "is_annotation": None,
                "mod_abbreviation": mod_abbreviation
            }

            file_upload(
                db=db,
                metadata=metadata,
                file=UploadFile(
                    file=BytesIO(markdown_content),
                    filename=f"{output_display_name}.md"
                ),
                upload_if_already_converted=True
            )

            successful_methods.append(method)
            logger.info(f"Uploaded {method} markdown for {reference_curie} ({file_class})")

        except Exception as e:
            logger.error(f"Failed to download/upload {method} for {reference_curie}: {e}")

    if successful_methods:
        logger.info(
            f"Successfully processed {file_class} PDF for {reference_curie} "
            f"with methods: {successful_methods}"
        )
        return True, successful_methods, None
    else:
        return False, [], "No methods successfully extracted"


def get_pdfs_missing_xml(
    db: Session,
    mod_abbreviation: Optional[str] = None,
    include_pmc: bool = True,
    limit: Optional[int] = None
) -> List[Dict]:
    """
    Find papers that have a main PDF but are missing nXML/TEI files.

    This function finds references that:
    1. Have a main PDF file (file_class='main', file_extension='pdf',
       file_publication_status='final')
    2. Do NOT have nXML files (file_class='nXML')
    3. Do NOT have TEI files (file_class='tei')
    4. Do NOT already have converted markdown files

    Filtered by MOD:
    - If mod_abbreviation is provided, includes PDFs where:
      - referencefile_mod.mod_id matches the given MOD, OR
      - referencefile_mod.mod_id is NULL (PMC/public PDFs) if include_pmc=True

    Args:
        db: Database session.
        mod_abbreviation: Optional MOD abbreviation to filter by (e.g., 'WB', 'MGI').
                         If None, returns PDFs for all MODs.
        include_pmc: If True and mod_abbreviation is provided, also include PDFs
                    with mod_id=NULL (typically from PMC). Default True.
        limit: Optional limit on number of results.

    Returns:
        List of dicts with reference and PDF info:
        [
            {
                "reference_id": int,
                "reference_curie": str,
                "referencefile_id": int,
                "display_name": str,
                "mod_abbreviation": str or None,
                "date_created": datetime
            },
            ...
        ]
    """
    from agr_literature_service.api.models import ModModel
    from agr_literature_service.api.models.referencefile_model import ReferencefileModAssociationModel
    from sqlalchemy import or_, select

    logger.info(
        f"Querying for PDFs missing XML files "
        f"(mod={mod_abbreviation or 'all'}, include_pmc={include_pmc})..."
    )

    # File classes that indicate XML is present
    xml_file_classes = ['nXML', 'tei']

    # File classes for markdown outputs (to exclude already processed)
    md_file_classes = list(EXTRACTION_METHODS.values())

    # Subquery: references that have XML files
    xml_exists_subquery = (
        select(ReferencefileModel.reference_id)
        .where(ReferencefileModel.file_class.in_(xml_file_classes))
        .distinct()
    )

    # Subquery: references that already have markdown files
    md_exists_subquery = (
        select(ReferencefileModel.reference_id)
        .where(ReferencefileModel.file_class.in_(md_file_classes))
        .distinct()
    )

    # Base query for main PDFs
    query = (
        db.query(ReferencefileModel)
        .filter(
            ReferencefileModel.file_class == "main",
            ReferencefileModel.file_extension == "pdf",
            ReferencefileModel.file_publication_status == "final",
            ~ReferencefileModel.reference_id.in_(xml_exists_subquery),
            ~ReferencefileModel.reference_id.in_(md_exists_subquery)
        )
    )

    # Apply MOD filter if specified
    if mod_abbreviation:
        # Get mod_id for the given abbreviation
        mod = db.query(ModModel).filter(
            ModModel.abbreviation == mod_abbreviation
        ).one_or_none()

        if not mod:
            logger.warning(f"MOD not found: {mod_abbreviation}")
            return []

        mod_id = mod.mod_id

        # Build the MOD filter condition
        if include_pmc:
            # Include PDFs with mod_id = given MOD OR mod_id is NULL
            mod_filter = or_(
                ReferencefileModAssociationModel.mod_id == mod_id,
                ReferencefileModAssociationModel.mod_id.is_(None)
            )
        else:
            # Only include PDFs with mod_id = given MOD
            mod_filter = ReferencefileModAssociationModel.mod_id == mod_id

        # Join with referencefile_mod and apply filter
        query = query.join(
            ReferencefileModAssociationModel,
            ReferencefileModel.referencefile_id == ReferencefileModAssociationModel.referencefile_id
        ).filter(mod_filter)

    # Order by date_created descending (newest first)
    query = query.order_by(ReferencefileModel.date_created.desc())

    if limit:
        query = query.limit(limit)

    # Execute query and build results
    results = []
    pdf_files = query.all()

    logger.info(f"Found {len(pdf_files)} PDFs missing XML files")

    for pdf_file in pdf_files:
        reference = db.query(ReferenceModel).filter(
            ReferenceModel.reference_id == pdf_file.reference_id
        ).one_or_none()

        if not reference:
            continue

        # Get MOD abbreviation from referencefile_mods
        file_mod_abbreviation = None
        for ref_file_mod in pdf_file.referencefile_mods:
            if ref_file_mod.mod:
                file_mod_abbreviation = ref_file_mod.mod.abbreviation
                break

        results.append({
            "reference_id": reference.reference_id,
            "reference_curie": reference.curie,
            "referencefile_id": pdf_file.referencefile_id,
            "display_name": pdf_file.display_name,
            "mod_abbreviation": file_mod_abbreviation,
            "date_created": pdf_file.date_created
        })

    return results
