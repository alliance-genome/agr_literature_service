import os
import re
import io
import tarfile
import zipfile
from pathlib import Path
from typing import Dict, Any, List, Tuple, BinaryIO
import logging

logger = logging.getLogger(__name__)


def _get_curie(ref_id: str, mod_abbreviation: str) -> str:
    """
    Generate a CURIE based on the reference ID and MOD.

    - 15-digit numeric IDs → AGRKB:{ref_id}
    - WB → WB:WBPaper{ref_id}
    - All others → PMID:{ref_id}
    """
    # 15-digit Alliance IDs
    if len(ref_id) == 15 and ref_id.isdigit():
        return f"AGRKB:{ref_id}"
    # MOD-specific: WB
    if mod_abbreviation.upper() == "WB":
        return f"WB:WBPaper{ref_id}"
    # Default for FB, SGD, MGI, RGD, ZFIN, XB, etc.
    return f"PMID:{ref_id}"


def parse_filename_by_mod(filename: str, mod_abbreviation: str) -> Dict[str, Any]:
    """
    Parse a main-file name into metadata according to MOD conventions.

    Patterns:
      {number}_{authorYear}[_{options}].{ext}
      {number}.{ext}

    Returns metadata including reference_curie, display_name, file_extension,
    file_publication_status, pdf_type, author_and_year, and mod_abbreviation.
    """
    name = Path(filename)
    stem = name.stem
    ext = name.suffix.lstrip('.')

    # Try pattern: number_authorYear[_options]
    m = re.match(r"^(?P<ref>\d+)_(?P<year>[^_]+)(?:_(?P<opts>.*))?$", stem)
    if m:
        ref_id = m.group('ref')
        author_and_year = m.group('year')
        opts = m.group('opts') or ''
    else:
        # Fallback: numbers only
        m2 = re.match(r"^(?P<ref>\d+)$", stem)
        if not m2:
            raise ValueError(
                f"Filename '{filename}' does not match expected patterns: '1234_ab23_opt.ext' or '1234.ext'"
            )
        ref_id = m2.group('ref')
        author_and_year = ''
        opts = ''

    reference_curie = _get_curie(ref_id, mod_abbreviation)

    # Determine status and PDF type
    pub_status = 'final'
    pdf_type = None
    opt_lower = opts.lower()
    if opt_lower:
        if opt_lower == 'temp':
            pub_status = 'temp'
        elif opt_lower in {'aut', 'ocr', 'html', 'htm', 'lib', 'tif'}:
            pdf_type = 'html' if opt_lower in {'html', 'htm'} else opt_lower

    return {
        'reference_curie': reference_curie,
        'display_name': stem,
        'file_extension': ext,
        'file_publication_status': pub_status,
        'pdf_type': pdf_type,
        'author_and_year': author_and_year,
        'mod_abbreviation': mod_abbreviation,
    }


def parse_supplement_file(filename: str, reference_dir: str, mod_abbreviation: str) -> Dict[str, Any]:
    """
    Build metadata for supplement files; reference_dir is parent folder name.
    """
    name = Path(filename)
    stem = name.stem
    ext = name.suffix.lstrip('.')

    ref_id = reference_dir
    reference_curie = _get_curie(ref_id, mod_abbreviation)

    return {
        'reference_curie': reference_curie,
        'display_name': stem,
        'file_extension': ext,
        'file_publication_status': 'final',
        'pdf_type': None,
        'mod_abbreviation': mod_abbreviation,
    }


def classify_and_parse_file(
    file_path: str,
    archive_root: str,
    mod_abbreviation: str
) -> Dict[str, Any]:
    """
    Classify a file as 'main' or 'supplement' by its relative path, then parse metadata.
    """
    rel_path = Path(file_path).relative_to(archive_root)
    parts = rel_path.parts

    if len(parts) == 1:
        meta = parse_filename_by_mod(parts[0], mod_abbreviation)
        file_class = 'main'
    else:
        meta = parse_supplement_file(parts[-1], parts[0], mod_abbreviation)
        file_class = 'supplement'

    meta['file_class'] = file_class
    meta['is_annotation'] = False
    return meta


def extract_and_classify_files(
    archive_file: BinaryIO,
    temp_dir: str
) -> List[Tuple[str, bool]]:
    """
    Extract an archive (.tar.gz or .zip) into temp_dir and return a list of
    (absolute_path, is_main) tuples.
    """
    extracted: List[Tuple[str, bool]] = []
    archive_file.seek(0)

    def _process(entries, is_tar: bool):
        for entry in entries:
            name = entry.name if is_tar else entry.filename
            if (is_tar and entry.isfile()) or (not is_tar and not entry.is_dir()):
                full = os.path.join(temp_dir, name)
                main = len(Path(name).parts) == 1
                extracted.append((full, main))

    # Try tar.gz
    try:
        with tarfile.open(fileobj=archive_file, mode='r:gz') as tar:
            tar.extractall(path=temp_dir)
            _process(tar.getmembers(), True)
        logger.info(f"Extracted {len(extracted)} files from tar.gz archive")
        return extracted
    except tarfile.TarError:
        archive_file.seek(0)
        # Fallback to zip
        try:
            with zipfile.ZipFile(archive_file) as zf:
                zf.extractall(path=temp_dir)
                _process(zf.filelist, False)
            logger.info(f"Extracted {len(extracted)} files from zip archive")
            return extracted
        except zipfile.BadZipFile:
            raise ValueError("Unsupported archive format; use .tar.gz or .zip")


def process_single_file(file_path: str, metadata: Dict[str, Any], db) -> Dict[str, Any]:
    """
    Read a file from disk and delegate to referencefile_crud.file_upload.
    """
    filename = Path(file_path).name
    try:
        with open(file_path, 'rb') as f:
            data = f.read()

        class MockUploadFile:
            def __init__(self, name: str, content: bytes):
                self.filename = name
                self.content_type = 'application/octet-stream'
                self.file = io.BytesIO(content)
                self.headers: Dict[str, str] = {}

        upload = MockUploadFile(filename, data)
        from agr_literature_service.api.crud.referencefile_crud import file_upload
        file_upload(db, metadata, upload, upload_if_already_converted=True)  # type: ignore

        logger.info(f"Successfully uploaded {filename} to {metadata['reference_curie']}")
        return {'status': 'success', 'reference_curie': metadata['reference_curie'], 'file_class': metadata['file_class']}
    except Exception as e:
        logger.error(f"Error uploading {filename}: {e}")
        return {'status': 'error', 'error': str(e), 'reference_curie': metadata.get('reference_curie'), 'file_class': metadata.get('file_class')}


def validate_archive_structure(archive_file: BinaryIO) -> Dict[str, Any]:
    """
    Inspect an archive to count main vs. supplement files without extracting to disk.
    """
    archive_file.seek(0)
    file_list: List[str] = []

    try:
        with tarfile.open(fileobj=archive_file, mode='r:gz') as tar:
            file_list = [m.name for m in tar.getmembers() if m.isfile()]
    except tarfile.TarError:
        archive_file.seek(0)
        try:
            with zipfile.ZipFile(archive_file) as zf:
                file_list = [f.filename for f in zf.filelist if not f.is_dir()]
        except zipfile.BadZipFile:
            return {'valid': False, 'error': 'Unsupported archive type', 'total_files': 0, 'main_files': 0, 'supplement_files': 0}

    main = [p for p in file_list if len(Path(p).parts) == 1]
    supp = [p for p in file_list if len(Path(p).parts) > 1]

    return {
        'valid': True,
        'total_files': len(file_list),
        'main_files': len(main),
        'supplement_files': len(supp),
        'main_file_list': main[:10],
        'supplement_file_list': supp[:10],
    }
