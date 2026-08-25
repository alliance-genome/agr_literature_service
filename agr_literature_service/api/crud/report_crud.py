"""
report_crud.py

Read-only access to the report/log files the automated pipelines write under
LOG_PATH. The same host directory is bound into this container and into the
reverse proxy that publishes it at /reports, so the files can be listed straight
off local disk rather than by scraping the proxy's directory index.

Nothing here modifies anything on disk.
==============
"""
import logging
from datetime import datetime, timezone
from os import SEEK_END, environ, path, stat as os_stat, walk

from fastapi import HTTPException, status

logger = logging.getLogger(__name__)


def _log_root():
    """Resolved LOG_PATH, or None when it is unset or missing."""
    log_path = environ.get('LOG_PATH')
    if not log_path:
        logger.warning("LOG_PATH is not set; no report files can be listed.")
        return None
    root = path.realpath(log_path)
    if not path.isdir(root):
        logger.warning("LOG_PATH %s is not a directory; no report files can be listed.", root)
        return None
    return root


def _public_url(relative_path):
    """Public href for a report file, or None when LOG_URL is not configured."""
    log_url = environ.get('LOG_URL')
    if not log_url:
        return None
    return log_url.rstrip('/') + '/' + relative_path


def _describe(full_path, relative_path):
    info = os_stat(full_path)
    modified = datetime.fromtimestamp(info.st_mtime, tz=timezone.utc)
    return {
        "path": relative_path,
        "name": path.basename(relative_path),
        "directory": path.dirname(relative_path),
        "size": info.st_size,
        "modified": modified.strftime('%Y-%m-%dT%H:%M:%SZ'),
        "url": _public_url(relative_path),
    }


def list_report_files():
    """Every report file under LOG_PATH, as a flat list of metadata dicts.

    The whole tree is a few hundred files, so it is returned unpaginated and the
    UI does its own filtering; that keeps every facet change instant.
    """
    root = _log_root()
    if root is None:
        return []

    entries = []
    for dirpath, _dirnames, filenames in walk(root):
        for filename in filenames:
            full_path = path.join(dirpath, filename)
            relative_path = path.relpath(full_path, root).replace(path.sep, '/')
            try:
                entries.append(_describe(full_path, relative_path))
            except OSError:
                # A file removed by log rotation between walk() and stat(), or a
                # broken symlink. Skip it rather than failing the whole listing.
                logger.warning("Could not stat report file %s; skipping.", relative_path)

    return sorted(entries, key=lambda entry: entry["path"])


def _resolve_within_root(relative_path):
    """Absolute path for a caller-supplied report path, refusing any escape.

    The root is read from LOG_PATH here rather than accepted as an argument, so
    the containment check can only ever be made against the configured report
    directory and no caller can hand it a root of its own choosing.

    realpath is what does the work: it collapses .. segments and follows
    symlinks, so a link inside LOG_PATH that points elsewhere is caught too.
    Never interpolate the caller's string into a path without this check.
    """
    root = _log_root()
    if root is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="Report files are not available on this host.")
    if not relative_path or not isinstance(relative_path, str):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="A report file path is required.")
    if path.isabs(relative_path):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Report file path must be relative to the report directory.")

    candidate = path.realpath(path.join(root, relative_path))
    if candidate != root and not candidate.startswith(root + path.sep):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Report file path is outside the report directory.")
    return candidate


def get_report_file(relative_path, tail=None):
    """One report file's text, optionally only its last ``tail`` bytes.

    Tailing keeps the multi-megabyte logs usable: the largest of them is tens of
    MB, which no browser should be asked to hold just to show the end of a run.
    """
    full_path = _resolve_within_root(relative_path)
    if not path.isfile(full_path):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"No report file at {relative_path}.")

    size = os_stat(full_path).st_size
    truncated = False
    with open(full_path, 'rb') as handle:
        if tail and tail > 0 and size > tail:
            handle.seek(-tail, SEEK_END)
            truncated = True
        data = handle.read()

    return {
        "path": relative_path,
        "name": path.basename(full_path),
        "size": size,
        "truncated": truncated,
        # Report files are plain text but occasionally carry stray bytes from a
        # crashed job; replace rather than 500 on the whole request.
        "content": data.decode('utf-8', errors='replace'),
    }
