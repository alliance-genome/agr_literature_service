"""
Utility functions for PDF to Markdown conversion.

This module provides helper functions for processing PDFs for individual papers,
including PDFX API integration, reference lookup by curie, and PDF file retrieval.
"""
import logging
import os
import time
from io import BytesIO
from typing import Any, Dict, List, Literal, Optional, Tuple, TypedDict

import requests
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
from agr_cognito_py import ModAccess, get_authentication_token
from agr_cognito_py.config import CognitoAdminConfig


logger = logging.getLogger(__name__)

# Extraction methods and their corresponding file classes
EXTRACTION_METHODS: Dict[str, str] = {
    "grobid": "converted_grobid_main",
    "docling": "converted_docling_main",
    "marker": "converted_marker_main",
    "merged": "converted_merged_main",
}


class PdfDetail(TypedDict):
    """Type for PDF processing detail."""
    referencefile_id: int
    display_name: str
    file_class: str
    success: bool
    methods_uploaded: List[str]
    error: Optional[str]


class ProcessingResult(TypedDict):
    """Type for PDF processing result."""
    success: bool
    reference_curie: Optional[str]
    input_curie: str
    pdfs_processed: int
    pdfs_succeeded: int
    pdfs_failed: int
    details: List[PdfDetail]
    error: Optional[str]


# Token cache for PDFX API
_token_cache: Dict[str, Any] = {
    "token": None,
    "expires_at": 0
}


def get_pdfx_token() -> str:  # pragma: no cover
    """
    Obtain PDFX bearer token using Cognito client_credentials grant.
    Uses agr_cognito_py with custom CognitoAdminConfig for PDFX credentials.
    Token is cached and refreshed when expired.

    Environment variables:
        PDFX_CLIENT_ID: Cognito client ID for PDFX service.
        PDFX_CLIENT_SECRET: Cognito client secret for PDFX service.
        PDFX_TOKEN_URL: OAuth token endpoint (default: https://auth.alliancegenome.org/oauth2/token).
        PDFX_SCOPE: OAuth scope (default: pdfx-api/extract).

    Returns:
        str: The access token for PDFX API authentication.

    Raises:
        ValueError: If required environment variables are not set.
        requests.RequestException: If token request fails.
    """
    current_time = time.time()

    # Return cached token if still valid (with 60 second buffer)
    if _token_cache["token"] and _token_cache["expires_at"] > current_time + 60:
        return _token_cache["token"]

    client_id = os.environ.get("PDFX_CLIENT_ID")
    client_secret = os.environ.get("PDFX_CLIENT_SECRET")
    token_url = os.environ.get(
        "PDFX_TOKEN_URL", "https://auth.alliancegenome.org/oauth2/token"
    )
    scope = os.environ.get("PDFX_SCOPE", "pdfx-api/extract")

    if not client_id or not client_secret:
        raise ValueError(
            "PDFX_CLIENT_ID and PDFX_CLIENT_SECRET environment variables must be set"
        )

    # Use agr_cognito_py with custom config for PDFX
    config = CognitoAdminConfig(
        client_id=client_id,
        client_secret=client_secret,
        token_url=token_url,
        scope=scope
    )

    token = get_authentication_token(config)

    # Cache the token (agr_cognito_py tokens typically expire in 3600 seconds)
    _token_cache["token"] = token
    _token_cache["expires_at"] = current_time + 3600

    return token


def submit_pdf_to_pdfx(  # pragma: no cover
    file_content: bytes,
    token: str,
    methods: str = "grobid,docling,marker",
    merge: bool = True,
    reference_curie: Optional[str] = None,
    mod_abbreviation: Optional[str] = None,
    max_retries: int = 3,
    retry_delay: int = 5
) -> str:
    """
    Submit a PDF to the PDFX service for processing.

    Args:
        file_content: The PDF file content as bytes.
        token: The bearer token for authentication.
        methods: Comma-separated extraction methods to use.
        merge: Whether to generate merged output.
        reference_curie: Optional reference identifier for logging.
        mod_abbreviation: Optional MOD abbreviation for logging.
        max_retries: Maximum number of retry attempts for transient failures.
        retry_delay: Delay between retries in seconds.

    Returns:
        str: The process_id for tracking the extraction job.

    Raises:
        requests.RequestException: If submission fails after all retries.
    """
    pdfx_api_url = os.environ.get("PDFX_API_URL", "https://pdfx.alliancegenome.org")
    url = f"{pdfx_api_url}/api/v1/extract"

    headers = {
        "Authorization": f"Bearer {token}"
    }

    # Prepare multipart form data
    data = {
        "methods": methods,
        "merge": str(merge).lower()
    }

    logger.info(f"Submitting PDF to PDFX for {reference_curie or 'unknown reference'}")

    last_exception = None
    for attempt in range(max_retries):
        try:
            # Recreate files dict for each attempt (file pointer may be exhausted)
            files = {
                "file": ("document.pdf", file_content, "application/pdf")
            }
            response = requests.post(
                url, headers=headers, files=files, data=data, timeout=120
            )
            response.raise_for_status()

            result = response.json()
            process_id = result.get("process_id")

            if not process_id:
                raise ValueError(f"No process_id returned from PDFX: {result}")

            logger.info(f"PDFX submission successful. Process ID: {process_id}")
            return process_id

        except (requests.exceptions.SSLError,
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as e:
            last_exception = e
            if attempt < max_retries - 1:
                logger.warning(
                    f"PDFX submission attempt {attempt + 1} failed: {e}. "
                    f"Retrying in {retry_delay}s..."
                )
                time.sleep(retry_delay)
            else:
                logger.error(f"PDFX submission failed after {max_retries} attempts")
                raise

    # This should never be reached if max_retries >= 1, but satisfy mypy
    if last_exception is not None:
        raise last_exception
    raise RuntimeError("PDFX submission failed: no exception captured")


def poll_pdfx_status(  # pragma: no cover
    process_id: str,
    token: str,
    timeout: int = 900,
    poll_interval: int = 10
) -> Dict:
    """
    Poll the PDFX service for job completion.

    Args:
        process_id: The process ID to check.
        token: The bearer token for authentication.
        timeout: Maximum time to wait in seconds (default 15 minutes).
        poll_interval: Time between polls in seconds.

    Returns:
        dict: The status response containing job state and results.

    Raises:
        TimeoutError: If job doesn't complete within timeout.
        requests.RequestException: If polling request fails.
    """
    pdfx_api_url = os.environ.get("PDFX_API_URL", "https://pdfx.alliancegenome.org")
    url = f"{pdfx_api_url}/api/v1/extract/{process_id}"

    headers = {
        "Authorization": f"Bearer {token}"
    }

    start_time = time.time()
    last_status = None

    while time.time() - start_time < timeout:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()

        status_data = response.json()
        current_status = status_data.get("status")

        if current_status != last_status:
            logger.info(f"PDFX job {process_id} status: {current_status}")
            last_status = current_status

        if current_status in ["completed", "complete"]:
            return status_data
        elif current_status == "failed":
            error_msg = status_data.get("error", "Unknown error")
            raise RuntimeError(f"PDFX job failed: {error_msg}")
        elif current_status in [
            "queued", "warming", "processing", "progress", "pending", "started"
        ]:
            time.sleep(poll_interval)
        else:
            logger.warning(f"Unknown PDFX status: {current_status}")
            time.sleep(poll_interval)

    raise TimeoutError(f"PDFX job {process_id} timed out after {timeout} seconds")


def download_pdfx_result(process_id: str, method: str, token: str) -> bytes:  # pragma: no cover
    """
    Download the markdown result for a specific extraction method.

    Args:
        process_id: The process ID of the completed job.
        method: The extraction method (grobid, docling, marker, merged).
        token: The bearer token for authentication.

    Returns:
        bytes: The markdown content.

    Raises:
        requests.RequestException: If download fails.
    """
    pdfx_api_url = os.environ.get("PDFX_API_URL", "https://pdfx.alliancegenome.org")
    url = f"{pdfx_api_url}/api/v1/extract/{process_id}/download/{method}"

    headers = {
        "Authorization": f"Bearer {token}"
    }

    response = requests.get(url, headers=headers, timeout=60)
    response.raise_for_status()

    return response.content


# Valid pdf_type values
PdfType = Literal["main", "supplement", "both"]


def resolve_curie_to_reference(db: Session, curie: str) -> Optional[ReferenceModel]:  # pragma: no cover
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


def get_pdf_files_for_reference(  # pragma: no cover
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


def process_pdf_for_reference(  # pragma: no cover
    db: Session,
    curie: str,
    pdf_type: PdfType = "main",
    methods_to_extract: Optional[List[str]] = None,
    token: Optional[str] = None
) -> ProcessingResult:
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

    # Use explicit typed variables for counters and lists
    pdfs_processed: int = 0
    pdfs_succeeded: int = 0
    pdfs_failed: int = 0
    details: List[PdfDetail] = []
    error_msg: Optional[str] = None

    # Resolve curie to reference
    reference = resolve_curie_to_reference(db, curie)
    if not reference:
        error_msg = f"Could not resolve curie to reference: {curie}"
        logger.error(error_msg)
        return ProcessingResult(
            success=False,
            reference_curie=None,
            input_curie=curie,
            pdfs_processed=0,
            pdfs_succeeded=0,
            pdfs_failed=0,
            details=[],
            error=error_msg
        )

    # After this point, reference is guaranteed to be non-None
    reference_curie: str = reference.curie

    # Get PDF files
    pdf_files = get_pdf_files_for_reference(db, reference.reference_id, pdf_type)
    if not pdf_files:
        error_msg = f"No PDF files found for {reference_curie} with pdf_type='{pdf_type}'"
        logger.warning(error_msg)
        return ProcessingResult(
            success=False,
            reference_curie=reference_curie,
            input_curie=curie,
            pdfs_processed=0,
            pdfs_succeeded=0,
            pdfs_failed=0,
            details=[],
            error=error_msg
        )

    logger.info(f"Found {len(pdf_files)} PDF file(s) for {reference_curie} (pdf_type={pdf_type})")

    # Get token if not provided
    if token is None:
        try:
            token = get_pdfx_token()
        except Exception as e:
            error_msg = f"Failed to obtain PDFX token: {e}"
            logger.error(error_msg)
            return ProcessingResult(
                success=False,
                reference_curie=reference_curie,
                input_curie=curie,
                pdfs_processed=0,
                pdfs_succeeded=0,
                pdfs_failed=0,
                details=[],
                error=error_msg
            )

    # Process each PDF file
    for pdf_file in pdf_files:
        pdfs_processed += 1
        pdf_detail: PdfDetail = {
            "referencefile_id": pdf_file.referencefile_id,
            "display_name": pdf_file.display_name,
            "file_class": pdf_file.file_class,
            "success": False,
            "methods_uploaded": [],
            "error": None
        }

        try:
            success, methods_uploaded, detail_error = _process_single_pdf_file(
                db=db,
                pdf_file=pdf_file,
                reference_curie=reference_curie,
                token=token,
                methods_to_extract=methods_to_extract
            )

            pdf_detail["success"] = success
            pdf_detail["methods_uploaded"] = methods_uploaded
            pdf_detail["error"] = detail_error

            if success:
                pdfs_succeeded += 1
            else:
                pdfs_failed += 1

        except Exception as e:
            pdf_detail["error"] = str(e)
            pdfs_failed += 1
            logger.error(f"Error processing PDF {pdf_file.display_name}: {e}")

        details.append(pdf_detail)

    # Overall success if at least one PDF was successfully processed
    return ProcessingResult(
        success=pdfs_succeeded > 0,
        reference_curie=reference_curie,
        input_curie=curie,
        pdfs_processed=pdfs_processed,
        pdfs_succeeded=pdfs_succeeded,
        pdfs_failed=pdfs_failed,
        details=details,
        error=None
    )


def _process_single_pdf_file(  # pragma: no cover
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


def get_pdfs_missing_xml(  # pragma: no cover
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
