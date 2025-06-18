import os
import re
import io
import tarfile
import zipfile
import gzip
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
    Returns only schema-allowed fields.
    """
    name = Path(filename)
    stem = name.stem
    ext = name.suffix.lstrip('.')

    # Try pattern: number_authorYear[_options]
    m = re.match(r"^(?P<ref>\d+)_(?P<year>[^_]+)(?:_(?P<opts>.*))?$", stem)
    if m:
        ref_id = m.group('ref')
        opts = m.group('opts') or ''
    else:
        # Fallback: numbers only
        m2 = re.match(r"^(?P<ref>\d+)$", stem)
        if not m2:
            raise ValueError(
                f"Filename '{filename}' does not match expected patterns: '1234_ab23_opt.ext' or '1234.ext'"
            )
        ref_id = m2.group('ref')
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
        'mod_abbreviation': mod_abbreviation,
    }


def parse_supplement_file(filename: str, reference_dir: str, mod_abbreviation: str) -> Dict[str, Any]:
    """
    Build metadata for supplement files; reference_dir is parent folder name.
    Returns only schema-allowed fields.
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
    Returns only fields allowed by ReferencefileSchemaPost, with a WB-specific fallback.

    Args:
        file_path: full path on disk to the extracted file
        archive_root: root directory path where the archive was unpacked
        mod_abbreviation: the MOD code (e.g. 'WB', 'SGD')

    Returns:
        A dict containing metadata keys:
        reference_curie, display_name, file_extension,
        file_publication_status, pdf_type, mod_abbreviation,
        file_class, is_annotation.
    """
    # Determine the path relative to the archive root
    rel_path = Path(file_path).relative_to(archive_root)
    parts = rel_path.parts

    try:
        # Main file if no subdirectory, else supplement
        if len(parts) == 1:
            meta = parse_filename_by_mod(parts[0], mod_abbreviation)
            file_class = 'main'
        else:
            meta = parse_supplement_file(parts[-1], parts[0], mod_abbreviation)
            file_class = 'supplement'
    except ValueError as ve:
        # WB-specific fallback for unexpected names
        if mod_abbreviation.upper() == "WB":
            # Use directory name or filename stem as the paper ID
            if len(parts) > 1:
                ref_id = parts[0]
            else:
                ref_id = Path(parts[0]).stem

            reference_curie = f"WB:WBPaper{ref_id}"
            display_name = Path(parts[-1]).stem
            ext = Path(parts[-1]).suffix.lstrip('.')

            meta = {
                'reference_curie': reference_curie,
                'display_name': display_name,
                'file_extension': ext,
                'file_publication_status': 'final',
                'pdf_type': None,
                'mod_abbreviation': mod_abbreviation,
            }
            file_class = 'main' if len(parts) == 1 else 'supplement'
            logger.warning(
                f"parse_filename_by_mod fallback for WB on '{parts[-1]}': {ve}"
            )
        else:
            # Re-raise for other MODs
            raise

    # Ensure these two fields always present
    meta['file_class'] = file_class
    meta['is_annotation'] = False

    # Only keep allowed schema fields
    allowed_fields = {
        'reference_curie', 'display_name', 'file_extension',
        'file_publication_status', 'pdf_type', 'mod_abbreviation',
        'file_class', 'is_annotation'
    }
    return {k: v for k, v in meta.items() if k in allowed_fields}


def extract_and_classify_files(archive_file: BinaryIO, temp_dir: str, archive_name: str = None) -> List[Tuple[str, bool]]:      # noqa: C901
    """
    Extract PDF, tar(.gz/.tgz/.bz2), zip, or gz.
    Return a list of (full_disk_path, is_main_file).
    """
    # 1) Read everything into memory
    archive_file.seek(0)
    data = archive_file.read()
    if not data:
        raise ValueError("Archive is empty")

    os.makedirs(temp_dir, exist_ok=True)
    extracted: List[Tuple[str, bool]] = []

    # Helper to skip macOS metadata / hidden files
    def _is_metadata(parts: Tuple[str, ...]) -> bool:
        if parts[0] == "__MACOSX":
            return True
        if parts[-1] == ".DS_Store":
            return True
        if any(p.startswith("._") for p in parts):
            return True
        if any(p.startswith(".") for p in parts):
            return True
        return False

    # 2) PDF?
    if data[:5] == b"%PDF-":
        out = os.path.join(temp_dir, archive_name or "file.pdf")
        with open(out, "wb") as f:
            f.write(data)
        return [(out, True)]

    # Helper to strip one common root folder
    def _strip_root(names: List[str]) -> List[str]:
        roots = {Path(n).parts[0] for n in names if n}
        if len(roots) == 1 and not Path(next(iter(roots))).suffix:
            root = next(iter(roots))
            stripped = []
            for n in names:
                parts = list(Path(n).parts)
                if parts[0] == root:
                    parts = parts[1:]
                if parts:
                    stripped.append(os.path.normpath(os.path.join(*parts)))
            return stripped
        return names

    # 3) Try TAR (handles .tar, .tar.gz, .tgz, .tar.bz2)
    try:
        bio = io.BytesIO(data)
        with tarfile.open(fileobj=bio, mode="r:*") as tar:
            members = [m for m in tar.getmembers() if m.isreg()]  # only regular files
            names = [m.name for m in members]
        names = _strip_root(names)

        # Extract & classify
        bio = io.BytesIO(data)
        with tarfile.open(fileobj=bio, mode="r:*") as tar:
            for m in members:
                orig = m.name
                parts = Path(orig).parts
                if _is_metadata(parts):
                    continue

                # classification is by the original path
                is_main = (len(parts) == 1)

                # build a stripped-on-disk path
                rel = orig
                # if there was exactly one top-level folder, drop it on disk
                root_candidates = {Path(n).parts[0] for n in names if n}
                if len(root_candidates) == 1 and Path(orig).parts[0] in root_candidates:
                    rel = os.path.normpath(os.path.join(*Path(orig).parts[1:]))

                full = os.path.join(temp_dir, rel)
                os.makedirs(os.path.dirname(full), exist_ok=True)
                with tar.extractfile(m) as src, open(full, "wb") as dst:
                    dst.write(src.read())
                extracted.append((full, is_main))

        if extracted:
            return extracted
    except Exception:
        pass

    # 4) Try ZIP
    try:
        bio = io.BytesIO(data)
        with zipfile.ZipFile(bio) as zf:
            infos = [i for i in zf.infolist() if not i.is_dir()]
            names = [i.filename for i in infos]
        names = _strip_root(names)

        bio = io.BytesIO(data)
        with zipfile.ZipFile(bio) as zf:
            for info in infos:
                orig = info.filename
                parts = Path(orig).parts
                if _is_metadata(parts):
                    continue

                is_main = (len(parts) == 1)

                rel = orig
                root_candidates = {Path(n).parts[0] for n in names if n}
                if len(root_candidates) == 1 and Path(orig).parts[0] in root_candidates:
                    rel = os.path.normpath(os.path.join(*Path(orig).parts[1:]))

                full = os.path.join(temp_dir, rel)
                os.makedirs(os.path.dirname(full), exist_ok=True)
                with open(full, "wb") as dst:
                    dst.write(zf.read(orig))
                extracted.append((full, is_main))

        if extracted:
            return extracted
    except Exception:
        pass

    # 5) Try single-file GZIP
    try:
        ungz = gzip.decompress(data)
        out = os.path.join(temp_dir, archive_name or "file")
        with open(out, "wb") as f:
            f.write(ungz)
        return [(out, True)]
    except Exception:
        pass

    # 6) Nothing matched
    raise ValueError("Unsupported archive format—supported: PDF, tar, tgz, zip, gz.")


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
        logger.exception(f"Error uploading {filename}")
        return {'status': 'error', 'error': str(e)}


def validate_archive_structure(archive_file: BinaryIO) -> Dict[str, Any]:      # noqa: C901
    """
    Validate PDF or a compressed archive (tar*, zip, gz).
    Strips any single top-level folder and hidden files before counting.
    Returns a dict with keys:
      valid, total_files, main_files, supplement_files,
      main_file_list, supplement_file_list, (and error if not valid)
    """
    # Load entire upload into memory
    archive_file.seek(0)
    data = archive_file.read()
    if not data:
        return {
            'valid': False,
            'error': 'Empty upload',
            'total_files': 0,
            'main_files': 0,
            'supplement_files': 0,
            'main_file_list': [],
            'supplement_file_list': []
        }

    # Helper: count mains vs supplements
    def _count(paths: List[str]) -> Dict[str, Any]:
        mains, supps = [], []
        for p in paths:
            parts = Path(p).parts
            if len(parts) == 1 and Path(p).suffix:
                mains.append(p)
            elif len(parts) > 1:
                supps.append(p)
        return {
            'valid': True,
            'total_files': len(mains) + len(supps),
            'main_files': len(mains),
            'supplement_files': len(supps),
            'main_file_list': mains[:10],
            'supplement_file_list': supps[:10],
        }

    # Helper: strip single top-level folder & drop hidden files
    def _clean_list(names: List[str]) -> List[str]:
        roots = {Path(n).parts[0] for n in names if n}
        strip_root = None
        if len(roots) == 1 and not Path(next(iter(roots))).suffix:
            strip_root = next(iter(roots))
        cleaned = []
        for n in names:
            parts = Path(n).parts
            if not parts or parts[0].startswith('.') or parts[0] == '__MACOSX':
                continue
            if strip_root and parts[0] == strip_root:
                parts = parts[1:]
            if not parts:
                continue
            cleaned.append(os.path.normpath(os.path.join(*parts)))
        return cleaned

    # 1) PDF?
    pdf_stream = io.BytesIO(data)
    try:
        pdf_stream.seek(0)
        if pdf_stream.read(5) == b'%PDF-':
            return {
                'valid': True,
                'total_files': 1,
                'main_files': 1,
                'supplement_files': 0,
                'main_file_list': ['PDF file'],
                'supplement_file_list': []
            }
    except Exception:
        pass

    # 2) TAR (any compression: .tar, .tgz, .tar.gz, etc.)
    try:
        bio = io.BytesIO(data)
        with tarfile.open(fileobj=bio, mode='r:*') as tar:
            names = [m.name for m in tar.getmembers() if m.isfile()]
        names = _clean_list(names)
        if names:
            return _count(names)
    except Exception:
        pass

    # 3) ZIP
    try:
        bio = io.BytesIO(data)
        with zipfile.ZipFile(bio) as zf:
            names = [info.filename for info in zf.filelist if not info.is_dir()]
        names = _clean_list(names)
        if names:
            return _count(names)
    except Exception:
        pass

    # 4) single-file GZIP
    try:
        bio = io.BytesIO(data)
        with gzip.GzipFile(fileobj=bio) as gz:
            gz.read(1)  # test decompression
        return {
            'valid': True,
            'total_files': 1,
            'main_files': 1,
            'supplement_files': 0,
            'main_file_list': ['gzipped file'],
            'supplement_file_list': []
        }
    except Exception:
        pass

    # 5) Unsupported
    return {
        'valid': False,
        'error': 'Unsupported archive format—supported: PDF, tar, tgz, zip, gz.',
        'total_files': 0,
        'main_files': 0,
        'supplement_files': 0,
        'main_file_list': [],
        'supplement_file_list': []
    }


def is_pdf_file(file: BinaryIO) -> bool:
    """Simple PDF validation"""
    try:
        file.seek(0)
        header = file.read(5)
        return header == b'%PDF-'
    except Exception:
        return False


def validate_compressed_archive(file: BinaryIO) -> Dict[str, Any]:
    """
    Read the entire file into memory and then try:
      1) tar (any compression: .tar, .tar.gz, .tgz, .tar.bz2)
      2) zip
      3) single-file gzip (.gz)
    """
    # Read all bytes once
    file.seek(0)
    data = file.read()
    file.seek(0)

    # Helper to process a list of paths
    def _process_list(paths: List[str]) -> Dict[str, Any]:
        main = [p for p in paths if len(Path(p).parts) == 1]
        supp = [p for p in paths if len(Path(p).parts) > 1]
        return {
            'valid': True,
            'total_files': len(paths),
            'main_files': len(main),
            'supplement_files': len(supp),
            'main_file_list': main[:10],
            'supplement_file_list': supp[:10],
        }
    # 1) Try any-compression TAR
    try:
        bio = io.BytesIO(data)
        with tarfile.open(fileobj=bio, mode='r:*') as tar:
            paths = [m.name for m in tar.getmembers() if m.isfile()]
        return _process_list(paths)
    except (tarfile.TarError, EOFError):
        pass

    # 2) Try ZIP
    try:
        bio = io.BytesIO(data)
        with zipfile.ZipFile(bio) as zf:
            paths = [info.filename for info in zf.filelist if not info.is_dir()]
        return _process_list(paths)
    except (zipfile.BadZipFile, EOFError):
        pass

    # 3) Try single-file GZIP
    try:
        bio = io.BytesIO(data)
        with gzip.GzipFile(fileobj=bio) as gz:
            # just test decompression
            gz.read(1)
        return {
            'valid': True,
            'total_files': 1,
            'main_files': 1,
            'supplement_files': 0,
            'main_file_list': ['gzipped file'],
            'supplement_file_list': []
        }
    except (OSError, EOFError):
        pass

    # 4) Nothing matched
    return {
        'valid': False,
        'error': 'Unsupported archive format—supported: PDF, tar, tgz, zip, gz.',
        'total_files': 0,
        'main_files': 0,
        'supplement_files': 0,
        'main_file_list': [],
        'supplement_file_list': []
    }


def validate_compressed_archive_old(file: BinaryIO) -> Dict[str, Any]:
    """Validate compressed archive formats."""
    # Ensure we start at the beginning
    file.seek(0)

    # 1) Try any-compression TAR (handles .tar, .tar.gz, .tgz, .tar.bz2, etc.)
    try:
        with tarfile.open(fileobj=file, mode='r:*') as tar:
            members = [m for m in tar.getmembers() if m.isfile()]
            file_list = [m.name for m in members]
        return process_file_list(file_list)
    except tarfile.TarError:
        file.seek(0)

    # 2) Try ZIP
    try:
        with zipfile.ZipFile(file) as zf:
            file_list = [info.filename for info in zf.filelist if not info.is_dir()]
        return process_file_list(file_list)
    except zipfile.BadZipFile:
        file.seek(0)

    # 3) Try single-file GZIP
    try:
        # test decompression
        with gzip.GzipFile(fileobj=file) as gz:
            gz.read(1)
        return {
            'valid': True,
            'total_files': 1,
            'main_files': 1,
            'supplement_files': 0,
            'main_file_list': ['gzipped file'],
            'supplement_file_list': []
        }
    except Exception:
        file.seek(0)

    # 4) Nothing matched
    return {
        'valid': False,
        'error': 'Unsupported archive format—supported: PDF, tar, tgz, zip, gz.',
        'total_files': 0,
        'main_files': 0,
        'supplement_files': 0,
        'main_file_list': [],
        'supplement_file_list': [],
    }


def process_file_list(file_list: List[str]) -> Dict[str, Any]:
    """Process file list into validation result"""
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


def validate_tar(file: BinaryIO) -> Dict[str, Any]:
    """Validate tar-based archives"""
    with tarfile.open(fileobj=file, mode='r:*') as tar:
        file_list = [m.name for m in tar.getmembers() if m.isfile()]

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


def validate_zip(file: BinaryIO) -> Dict[str, Any]:
    """Validate zip archives"""
    with zipfile.ZipFile(file) as zf:
        file_list = [f.filename for f in zf.filelist if not f.is_dir()]

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


def validate_gzip(file: BinaryIO) -> Dict[str, Any]:
    """Validate gzip files"""
    try:
        # Try as gzipped tar first
        with tarfile.open(fileobj=file, mode='r:gz') as tar:
            file_list = [m.name for m in tar.getmembers() if m.isfile()]

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
    except tarfile.TarError:
        # Handle as single gzip file
        file.seek(0)
        return {
            'valid': True,
            'total_files': 1,
            'main_files': 1,
            'supplement_files': 0,
            'main_file_list': ['gzipped file'],
            'supplement_file_list': []
        }
