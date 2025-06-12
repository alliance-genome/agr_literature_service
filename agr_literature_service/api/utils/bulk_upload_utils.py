"""
Utility functions for bulk upload processing.
"""

import os
import re
import io
import tarfile
import zipfile
from pathlib import Path
from typing import Dict, Any, List, Tuple
import logging

logger = logging.getLogger(__name__)


def parse_filename_by_mod(filename: str, mod_abbreviation: str) -> Dict[str, Any]:
    """
    Parse filename using existing MOD-specific rules from upload_files.sh

    Args:
        filename: The filename to parse
        mod_abbreviation: MOD abbreviation (WB, FB, etc.)

    Returns:
        Dictionary with parsed metadata

    Raises:
        ValueError: If filename doesn't match expected patterns
    """

    # Extract base filename and extension
    base_name = os.path.splitext(filename)[0]
    file_extension = os.path.splitext(filename)[1][1:]  # Remove leading dot

    # Regex patterns from existing shell script (match the whole filename with extension)
    regex_with_details = r"^([0-9]+)[_]([^_]+)[_]?(.*)?\..*$"
    regex_numbers_only = r"^([0-9]+)\..*$"

    reference_id = None
    author_and_year = ""
    additional_options = ""

    # Parse filename (including extension)
    match_details = re.match(regex_with_details, filename)
    match_numbers = re.match(regex_numbers_only, filename)

    if match_details:
        reference_id = match_details.group(1)
        author_and_year = match_details.group(2)
        additional_options = match_details.group(3) or ""
    elif match_numbers:
        reference_id = match_numbers.group(1)
    else:
        raise ValueError(f"Filename '{filename}' does not match expected patterns. "
                         f"Expected: {{number}}_{{author_year}}[_{{options}}].{{ext}} or {{number}}.{{ext}}")

    # Convert to proper reference CURIE based on MOD
    if len(reference_id) == 15 and reference_id.isdigit():
        reference_curie = f"AGRKB:{reference_id}"
    elif mod_abbreviation == "WB":
        reference_curie = f"WB:WBPaper{reference_id}"
    elif mod_abbreviation == "FB":
        reference_curie = f"PMID:{reference_id}"
    else:
        # For other MODs (SGD, MGI, RGD, ZFIN, XB), always use AGRKB prefix
        reference_curie = f"AGRKB:{reference_id}"

    # Parse additional options for file metadata
    file_publication_status = "final"
    pdf_type = None

    if additional_options:
        additional_options_lower = additional_options.lower()
        if additional_options_lower == "temp":
            file_publication_status = "temp"
        elif additional_options_lower in ["aut", "ocr", "html", "htm", "lib", "tif"]:
            pdf_type = additional_options_lower
            if pdf_type == "htm":
                pdf_type = "html"

    return {
        "reference_curie": reference_curie,
        "display_name": base_name,
        "file_extension": file_extension,
        "file_publication_status": file_publication_status,
        "pdf_type": pdf_type,
        "author_and_year": author_and_year,
        "mod_abbreviation": mod_abbreviation
    }


def parse_supplement_file(filename: str, reference_dir: str, mod_abbreviation: str) -> Dict[str, Any]:
    """
    Parse supplement file metadata.
    Reference ID comes from parent directory name.
    """

    # Extract file extension
    base_name = os.path.splitext(filename)[0]
    file_extension = os.path.splitext(filename)[1][1:]  # Remove leading dot

    # Reference ID comes from directory name
    reference_id = reference_dir

    # Convert to proper reference CURIE based on MOD
    if len(reference_id) == 15 and reference_id.isdigit():
        reference_curie = f"AGRKB:{reference_id}"
    elif mod_abbreviation == "WB":
        reference_curie = f"WB:WBPaper{reference_id}"
    elif mod_abbreviation == "FB":
        reference_curie = f"PMID:{reference_id}"
    else:
        reference_curie = f"AGRKB:{reference_id}" if len(reference_id) == 15 else reference_id

    return {
        "reference_curie": reference_curie,
        "display_name": base_name,
        "file_extension": file_extension,
        "file_publication_status": "final",
        "pdf_type": None,
        "mod_abbreviation": mod_abbreviation
    }


def classify_and_parse_file(file_path: str, archive_root: str, mod_abbreviation: str) -> Dict[str, Any]:
    """
    Classify file as main or supplement and parse metadata.

    Rules:
    - Files in root directory = main files
    - Files in subdirectories = supplement files
    - Reference ID for supplements = parent directory name
    """

    # Get relative path from archive root
    rel_path = os.path.relpath(file_path, archive_root)
    path_parts = Path(rel_path).parts
    filename = os.path.basename(file_path)

    if len(path_parts) == 1:
        # File is in root directory = main file
        file_class = "main"
        metadata = parse_filename_by_mod(filename, mod_abbreviation)
    else:
        # File is in subdirectory = supplement file
        file_class = "supplement"
        parent_dir = path_parts[0]  # First directory is the reference ID
        metadata = parse_supplement_file(filename, parent_dir, mod_abbreviation)

    metadata["file_class"] = file_class
    metadata["is_annotation"] = False
    return metadata


def extract_and_classify_files(archive_file, temp_dir: str) -> List[Tuple[str, bool]]:
    """
    Extract archive and return list of (file_path, is_main_file) tuples.

    Args:
        archive_file: File-like object containing the archive
        temp_dir: Temporary directory to extract to

    Returns:
        List of (full_file_path, is_main_file) tuples

    Raises:
        ValueError: If archive format is not supported
    """

    extracted_files = []

    # Reset file pointer to beginning
    archive_file.seek(0)

    # Try tar.gz first
    try:
        with tarfile.open(fileobj=archive_file, mode="r:gz") as tar:
            tar.extractall(temp_dir)

            # Classify files based on directory structure
            for member in tar.getmembers():
                if member.isfile():
                    full_path = os.path.join(temp_dir, member.name)
                    # Check if file is in root directory
                    rel_path = os.path.relpath(member.name)
                    is_main = len(Path(rel_path).parts) == 1
                    extracted_files.append((full_path, is_main))

        logger.info(f"Extracted {len(extracted_files)} files from tar.gz archive")
        return extracted_files

    except tarfile.TarError:
        # Reset file pointer and try zip
        archive_file.seek(0)

        try:
            with zipfile.ZipFile(archive_file) as zip_file:
                zip_file.extractall(temp_dir)

                for file_info in zip_file.filelist:
                    if not file_info.is_dir():
                        full_path = os.path.join(temp_dir, file_info.filename)
                        # Check if file is in root directory
                        is_main = len(Path(file_info.filename).parts) == 1
                        extracted_files.append((full_path, is_main))

            logger.info(f"Extracted {len(extracted_files)} files from zip archive")
            return extracted_files

        except zipfile.BadZipFile:
            raise ValueError("Archive format not supported. Please use .tar.gz or .zip format")


def process_single_file(file_path: str, metadata: Dict[str, Any], db) -> Dict[str, Any]:
    """
    Process single file using existing referencefile_crud logic.

    Args:
        file_path: Path to the file to process
        metadata: File metadata dictionary
        db: Database session

    Returns:
        Dictionary with processing result
    """

    filename = os.path.basename(file_path)

    try:
        # Read file content
        with open(file_path, 'rb') as f:
            file_content = f.read()

        # Create mock UploadFile object that mimics FastAPI's UploadFile
        class MockUploadFile:
            def __init__(self, filename: str, content: bytes):
                self.filename = filename
                self.content_type = 'application/octet-stream'
                self.file = io.BytesIO(content)
                self.size = len(content)
                self.headers: Dict[str, str] = {}

        mock_upload = MockUploadFile(filename, file_content)

        # Use existing upload logic
        from agr_literature_service.api.crud import referencefile_crud

        referencefile_crud.file_upload(db, metadata, mock_upload, upload_if_already_converted=True)  # type: ignore

        logger.info(f"Successfully processed file: {filename} -> {metadata['reference_curie']}")
        return {
            "status": "success",
            "reference_curie": metadata["reference_curie"],
            "file_class": metadata["file_class"]
        }

    except Exception as e:
        logger.error(f"Error processing file {filename}: {str(e)}")
        return {
            "status": "error",
            "error": str(e),
            "reference_curie": metadata.get("reference_curie", "unknown"),
            "file_class": metadata.get("file_class", "unknown")
        }


def validate_archive_structure(archive_file) -> Dict[str, Any]:
    """
    Validate archive structure and return summary information.

    Args:
        archive_file: File-like object containing the archive

    Returns:
        Dictionary with validation results and file counts
    """

    archive_file.seek(0)

    try:
        # Try to extract file list without actually extracting
        file_list = []

        try:
            with tarfile.open(fileobj=archive_file, mode="r:gz") as tar:
                for member in tar.getmembers():
                    if member.isfile():
                        file_list.append(member.name)
        except tarfile.TarError:
            archive_file.seek(0)
            with zipfile.ZipFile(archive_file) as zip_file:
                for file_info in zip_file.filelist:
                    if not file_info.is_dir():
                        file_list.append(file_info.filename)

        # Analyze structure
        main_files = []
        supplement_files = []

        for file_path in file_list:
            path_parts = Path(file_path).parts
            if len(path_parts) == 1:
                main_files.append(file_path)
            else:
                supplement_files.append(file_path)

        return {
            "valid": True,
            "total_files": len(file_list),
            "main_files": len(main_files),
            "supplement_files": len(supplement_files),
            "main_file_list": main_files[:10],  # First 10 for preview
            "supplement_file_list": supplement_files[:10]  # First 10 for preview
        }

    except Exception as e:
        return {
            "valid": False,
            "error": str(e),
            "total_files": 0,
            "main_files": 0,
            "supplement_files": 0
        }
