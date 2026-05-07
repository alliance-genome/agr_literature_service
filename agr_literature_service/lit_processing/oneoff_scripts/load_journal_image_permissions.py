#!/usr/bin/env python3
"""
Load journal_permission.tsv into image_permission and resource_image_permission.

This loader is intentionally non-destructive:
- rows present in the TSV are inserted or updated
- existing database rows that are not present in the TSV are never removed
- dry-run is the default; pass --apply to commit changes

The script uses SQLAlchemy models so audited/version tables are populated by
the normal ORM/versioning machinery when --apply is used.

Usage:
    python load_journal_image_permissions.py
    python load_journal_image_permissions.py --apply
    python load_journal_image_permissions.py --report-file data/journal_permission_load_report.tsv
"""

import argparse
import csv
import hashlib
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from os import environ, path
from typing import Dict, Iterable, List, Optional, Tuple

from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from agr_literature_service.api.models import (
    ImagePermissionModel,
    ReferenceModel,
    ResourceImagePermissionModel,
    ResourceModel,
)
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logging.basicConfig(format="%(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DEFAULT_INPUT_FILE = "data/journal_permission.tsv"
DEFAULT_REPORT_FILE = "data/journal_permission_load_report.tsv"
SCRIPT_USER = path.basename(__file__).replace(".py", "")

MOD_PERMISSION_COLUMNS = ["FB", "MGI", "RGD", "SGD", "WB", "XB", "ZFIN"]
NOTE_COLUMNS = [
    "Comments",
    "Hybrid Journal",
    "Embargo",
    "Author permission required?",
    "Delay of Use",
]

POSITIVE_PERMISSION_PATTERNS = (
    "blanket",
    "cc by",
    "creative commons",
    "open access",
    "publisher permission",
    "permission to use images",
    "contract",
    "oa",
)

PERMISSION_URL_PATTERN = re.compile(
    r"(copyright|licens|open[-_]?access|permission|polic|reprint|rights)",
    flags=re.IGNORECASE,
)


@dataclass
class JournalPermissionRow:
    line_no: int
    curator_id: str
    data: Dict[str, str]
    journal_abbreviation: str
    publisher: str
    full_journal_name: str
    start_year: Optional[int]
    end_year: Optional[int]
    permission_name: str
    legacy_permission_name: str
    permission_text: str
    permission_url: Optional[str]
    can_display_images: bool
    notes: str


@dataclass
class ResourceLookup:
    resource_by_exact_abbreviation: Dict[str, List[ResourceModel]]
    resource_by_exact_title: Dict[str, List[ResourceModel]]
    resource_by_abbreviation: Dict[str, List[ResourceModel]]
    resource_by_title: Dict[str, List[ResourceModel]]
    resource_by_curie: Dict[str, ResourceModel]
    reference_count_by_resource_id: Dict[int, int]


@dataclass
class FailedRow:
    line_no: int
    curator_id: str
    journal_abbreviation: str
    publisher: str
    full_journal_name: str
    reason: str
    detail: str


@dataclass
class LoadStats:
    rows_seen: int = 0
    rows_skipped: int = 0
    resources_unmatched: int = 0
    resources_ambiguous: int = 0
    permissions_created: int = 0
    permissions_updated: int = 0
    permissions_unchanged: int = 0
    links_created: int = 0
    links_updated: int = 0
    links_unchanged: int = 0
    errors: int = 0
    failed_rows: List[FailedRow] = None

    def __post_init__(self) -> None:
        if self.failed_rows is None:
            self.failed_rows = []


def clean(value: Optional[str]) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def is_blankish(value: Optional[str]) -> bool:
    text = clean(value).lower()
    return text in {"", "n/a", "na", "none", "null", "(null)", "-"}


def normalize_for_match(value: str) -> str:
    text = clean(value).lower()
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return clean(text)


def normalize_exact(value: str) -> str:
    return clean(value).lower().rstrip(".")


def normalize_abbreviation(value: str) -> str:
    text = clean(value)
    text = re.sub(r"\s*\(.*$", "", text)
    text = re.sub(r"\s*[<>]\s*\d{4}.*$", "", text)
    text = text.rstrip(".")
    return normalize_for_match(text)


def parse_year_range(row: Dict[str, str]) -> Tuple[Optional[int], Optional[int]]:
    journal = clean(row.get("Journal (NLM abbrev)"))
    acknowledgement = clean(row.get("WB Acknowledgements"))
    search_text = f"{journal} {acknowledgement}"

    range_match = re.search(r"\b(1[89]\d{2}|20\d{2})\s*[-–]\s*(1[89]\d{2}|20\d{2})\b", search_text)
    if range_match:
        return int(range_match.group(1)), int(range_match.group(2))

    before_match = re.search(r"(?:<|before\s+)(1[89]\d{2}|20\d{2})", search_text, flags=re.IGNORECASE)
    if before_match:
        return None, int(before_match.group(1)) - 1

    after_match = re.search(r"(?:>|after\s+)(1[89]\d{2}|20\d{2})", search_text, flags=re.IGNORECASE)
    if after_match:
        return int(after_match.group(1)) + 1, None

    return None, None


def range_label(start_year: Optional[int], end_year: Optional[int]) -> str:
    if start_year is None and end_year is None:
        return "all years"
    if start_year is None:
        return f"through {end_year}"
    if end_year is None:
        return f"from {start_year}"
    return f"{start_year}-{end_year}"


def extract_urls(value: Optional[str]) -> List[str]:
    if is_blankish(value):
        return []
    return [url.rstrip(").,;") for url in re.findall(r"https?://[^\s;,]+", clean(value))]


def is_permission_url(url: str) -> bool:
    return bool(PERMISSION_URL_PATTERN.search(url))


def permission_url(row: Dict[str, str]) -> Optional[str]:
    urls = [url for url in extract_urls(row.get("Licensing Link")) if is_permission_url(url)]
    return "; ".join(urls) if urls else None


def build_permission_text(row: Dict[str, str]) -> str:
    acknowledgement = clean(row.get("WB Acknowledgements"))
    if not is_blankish(acknowledgement):
        return acknowledgement

    return ""


def build_notes(row: Dict[str, str]) -> str:
    notes = []
    curator_id = clean(row.get("na"))
    if not is_blankish(curator_id):
        notes.append(f"Curator ID: {curator_id}")

    for column in NOTE_COLUMNS:
        value = clean(row.get(column))
        if value and not is_blankish(value):
            notes.append(f"{column}: {value}")

    mod_values = []
    for column in MOD_PERMISSION_COLUMNS:
        value = clean(row.get(column))
        if value and not is_blankish(value):
            mod_values.append(f"{column}={value}")
    if mod_values:
        notes.append("MOD permissions: " + "; ".join(mod_values))

    return "\n".join(notes)


def has_positive_permission_signal(row: Dict[str, str], subset_can_display: bool) -> bool:
    values = []
    values.append(clean(row.get("WB Acknowledgements")))
    values.extend(clean(row.get(column)) for column in MOD_PERMISSION_COLUMNS)
    values.extend(clean(row.get(column)) for column in ["License type", "Hybrid Journal", "Comments"])
    combined = " ".join(value.lower() for value in values if value)

    if not combined:
        return False

    if "subset" in combined and not subset_can_display:
        strong_blanket_signal = "blanket" in combined or "open access" in combined or "creative commons" in combined
        if not strong_blanket_signal:
            return False

    return any(pattern in combined for pattern in POSITIVE_PERMISSION_PATTERNS)


def build_legacy_permission_name(row: Dict[str, str], start_year: Optional[int], end_year: Optional[int]) -> str:
    journal = clean(row.get("Journal (NLM abbrev)")) or clean(row.get("Full Journal Name")) or "unknown journal"
    publisher = clean(row.get("Publisher")) or "unknown publisher"
    return f"Journal image permission: {journal} | {publisher} | {range_label(start_year, end_year)}"


def build_permission_name(
    row: Dict[str, str],
    permission_text: str,
    permission_url_value: Optional[str],
    can_display_images: bool,
) -> str:
    publisher = clean(row.get("Publisher")) or "unknown publisher"
    fingerprint_source = "\n".join([
        publisher,
        permission_text,
        permission_url_value or "",
        str(can_display_images),
    ])
    fingerprint = hashlib.sha1(fingerprint_source.encode("utf-8")).hexdigest()[:8]
    return f"{publisher} image permission ({fingerprint})"


def parse_tsv(input_file: Path, subset_can_display: bool) -> Iterable[JournalPermissionRow]:
    with input_file.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        for line_no, row in enumerate(reader, start=2):
            normalized_row = {key: clean(value) for key, value in row.items()}
            journal = clean(normalized_row.get("Journal (NLM abbrev)"))
            publisher = clean(normalized_row.get("Publisher"))
            full_journal_name = clean(normalized_row.get("Full Journal Name"))
            if not journal and not full_journal_name:
                continue
            start_year, end_year = parse_year_range(normalized_row)
            permission_text = build_permission_text(normalized_row)
            permission_url_value = permission_url(normalized_row)
            can_display_images = has_positive_permission_signal(normalized_row, subset_can_display)
            yield JournalPermissionRow(
                line_no=line_no,
                curator_id=clean(normalized_row.get("na")),
                data=normalized_row,
                journal_abbreviation=journal,
                publisher=publisher,
                full_journal_name=full_journal_name,
                start_year=start_year,
                end_year=end_year,
                permission_name=build_permission_name(
                    normalized_row,
                    permission_text,
                    permission_url_value,
                    can_display_images,
                ),
                legacy_permission_name=build_legacy_permission_name(normalized_row, start_year, end_year),
                permission_text=permission_text,
                permission_url=permission_url_value,
                can_display_images=can_display_images,
                notes=build_notes(normalized_row),
            )


def load_env_file(env_file: Optional[Path]) -> None:
    if env_file is None:
        return
    if not env_file.exists():
        raise FileNotFoundError(f"Environment file does not exist: {env_file}")

    for raw_line in env_file.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        environ.setdefault(key, value)


def build_resource_lookup(db: Session) -> ResourceLookup:
    resources = db.query(ResourceModel).all()
    reference_counts = dict(
        db.query(ReferenceModel.resource_id, func.count(ReferenceModel.reference_id))
        .filter(ReferenceModel.resource_id.isnot(None))
        .group_by(ReferenceModel.resource_id)
        .all()
    )
    by_exact_abbreviation: Dict[str, List[ResourceModel]] = {}
    by_exact_title: Dict[str, List[ResourceModel]] = {}
    by_abbreviation: Dict[str, List[ResourceModel]] = {}
    by_title: Dict[str, List[ResourceModel]] = {}
    by_curie: Dict[str, ResourceModel] = {}

    for resource in resources:
        by_curie[resource.curie] = resource
        if resource.title_abbreviation:
            by_exact_abbreviation.setdefault(normalize_exact(resource.title_abbreviation), []).append(resource)
            by_abbreviation.setdefault(normalize_abbreviation(resource.title_abbreviation), []).append(resource)
        if resource.title:
            by_exact_title.setdefault(normalize_exact(resource.title), []).append(resource)
            by_title.setdefault(normalize_for_match(resource.title), []).append(resource)

    return ResourceLookup(
        resource_by_exact_abbreviation=by_exact_abbreviation,
        resource_by_exact_title=by_exact_title,
        resource_by_abbreviation=by_abbreviation,
        resource_by_title=by_title,
        resource_by_curie=by_curie,
        reference_count_by_resource_id=reference_counts,
    )


def choose_unique(candidates: List[ResourceModel]) -> Optional[ResourceModel]:
    if len(candidates) == 1:
        return candidates[0]
    if len(candidates) > 1:
        # Duplicate resources sometimes differ only by obsolete/import history.
        # Prefer the lowest resource_id for deterministic dry-run reporting, but
        # require manual review by treating the match as ambiguous.
        return None
    return None


def choose_active_resource(candidates: List[ResourceModel], lookup: ResourceLookup) -> Optional[ResourceModel]:
    active_candidates = [
        resource
        for resource in candidates
        if lookup.reference_count_by_resource_id.get(resource.resource_id, 0) > 0
    ]
    return choose_unique(active_candidates)


def choose_unique_by_title(candidates: List[ResourceModel], full_journal_name: str) -> Optional[ResourceModel]:
    if not full_journal_name:
        return None
    title_key = normalize_for_match(full_journal_name)
    title_matches = [
        resource
        for resource in candidates
        if normalize_for_match(resource.title or "") == title_key
    ]
    return choose_unique(title_matches)


def choose_active_by_title(
    candidates: List[ResourceModel],
    full_journal_name: str,
    lookup: ResourceLookup,
) -> Optional[ResourceModel]:
    if not full_journal_name:
        return None
    title_key = normalize_for_match(full_journal_name)
    title_matches = [
        resource
        for resource in candidates
        if normalize_for_match(resource.title or "") == title_key
    ]
    return choose_active_resource(title_matches, lookup)


def resolve_candidates(
    candidates: List[ResourceModel],
    match_label: str,
    row: JournalPermissionRow,
    lookup: ResourceLookup,
) -> Tuple[Optional[ResourceModel], Optional[str]]:
    resource = choose_unique(candidates)
    if resource:
        return resource, match_label
    resource = choose_active_resource(candidates, lookup)
    if resource:
        return resource, f"active {match_label}"
    resource = choose_unique_by_title(candidates, row.full_journal_name)
    if resource:
        return resource, f"{match_label} plus title"
    resource = choose_active_by_title(candidates, row.full_journal_name, lookup)
    if resource:
        return resource, f"active {match_label} plus title"
    if len(candidates) > 1:
        return None, f"ambiguous {match_label}"
    return None, None


def find_resource(
    row: JournalPermissionRow,
    lookup: ResourceLookup,
    manual_resource_map: Dict[str, str],
) -> Tuple[Optional[ResourceModel], str]:
    manual_key = row.journal_abbreviation or row.full_journal_name
    manual_curie = manual_resource_map.get(manual_key)
    if manual_curie:
        resource = lookup.resource_by_curie.get(manual_curie)
        if resource:
            return resource, "manual"
        return None, f"manual curie not found: {manual_curie}"

    candidate_sets = [
        (
            lookup.resource_by_exact_abbreviation.get(normalize_exact(row.journal_abbreviation), []),
            "exact title_abbreviation",
        ),
        (
            lookup.resource_by_exact_title.get(normalize_exact(row.full_journal_name), []),
            "exact title",
        ),
        (
            lookup.resource_by_abbreviation.get(normalize_abbreviation(row.journal_abbreviation), []),
            "title_abbreviation",
        ),
        (
            lookup.resource_by_title.get(normalize_for_match(row.full_journal_name), []),
            "title",
        ),
    ]
    for candidates, match_label in candidate_sets:
        resource, result = resolve_candidates(candidates, match_label, row, lookup)
        if result:
            return resource, result

    return None, "not found"


def candidate_labels_with_reference_counts(
    candidates: List[ResourceModel],
    lookup: ResourceLookup,
) -> str:
    return "; ".join(
        f"{resource.curie} ({resource.title_abbreviation or resource.title or 'no title'}, "
        f"references={lookup.reference_count_by_resource_id.get(resource.resource_id, 0)})"
        for resource in candidates
    )


def describe_resource_match_failure(
    row: JournalPermissionRow,
    lookup: ResourceLookup,
    manual_resource_map: Dict[str, str],
    match_reason: str,
) -> str:
    manual_key = row.journal_abbreviation or row.full_journal_name
    manual_curie = manual_resource_map.get(manual_key)
    if manual_curie:
        return f"manual resource_curie was supplied but not found: {manual_curie}"

    exact_abbreviation_key = normalize_exact(row.journal_abbreviation)
    exact_abbreviation_candidates = lookup.resource_by_exact_abbreviation.get(exact_abbreviation_key, [])
    if exact_abbreviation_candidates:
        return (
            f"{match_reason}; exact title_abbreviation candidates: "
            f"{candidate_labels_with_reference_counts(exact_abbreviation_candidates, lookup)}"
        )

    exact_title_key = normalize_exact(row.full_journal_name)
    exact_title_candidates = lookup.resource_by_exact_title.get(exact_title_key, [])
    if exact_title_candidates:
        return (
            f"{match_reason}; exact title candidates: "
            f"{candidate_labels_with_reference_counts(exact_title_candidates, lookup)}"
        )

    abbreviation_key = normalize_abbreviation(row.journal_abbreviation)
    abbreviation_candidates = lookup.resource_by_abbreviation.get(abbreviation_key, [])
    if abbreviation_candidates:
        return (
            f"{match_reason}; title_abbreviation candidates: "
            f"{candidate_labels_with_reference_counts(abbreviation_candidates, lookup)}"
        )

    title_key = normalize_for_match(row.full_journal_name)
    title_candidates = lookup.resource_by_title.get(title_key, [])
    if title_candidates:
        return (
            f"{match_reason}; title candidates: "
            f"{candidate_labels_with_reference_counts(title_candidates, lookup)}"
        )

    return (
        "no resource matched normalized title_abbreviation "
        f"'{abbreviation_key}' or title '{title_key}'"
    )


def read_manual_resource_map(map_file: Optional[Path]) -> Dict[str, str]:
    if map_file is None:
        return {}
    if not map_file.exists():
        raise FileNotFoundError(f"Manual resource map does not exist: {map_file}")

    mapping = {}
    with map_file.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        for line_no, row in enumerate(reader, start=2):
            journal = clean(row.get("Journal (NLM abbrev)") or row.get("journal") or row.get("journal_abbreviation"))
            curie = clean(row.get("resource_curie") or row.get("curie"))
            if not journal or not curie:
                logger.warning(f"Manual map line {line_no}: skipped; expected journal and resource_curie")
                continue
            mapping[journal] = curie
    return mapping


def update_permission_fields(permission: ImagePermissionModel, row: JournalPermissionRow) -> bool:
    changed = False
    desired = {
        "name": row.permission_name,
        "permission_text": row.permission_text,
        "permission_url": row.permission_url,
        "can_display_images": row.can_display_images,
    }
    for field, value in desired.items():
        if getattr(permission, field) != value:
            setattr(permission, field, value)
            changed = True
    return changed


def permission_fields_changed(permission: ImagePermissionModel, row: JournalPermissionRow) -> bool:
    return any([
        permission.name != row.permission_name,
        permission.permission_text != row.permission_text,
        permission.permission_url != row.permission_url,
        permission.can_display_images != row.can_display_images,
    ])


def update_link_fields(
    link: ResourceImagePermissionModel,
    row: JournalPermissionRow,
    image_permission_id: int,
) -> bool:
    changed = False
    if link.image_permission_id != image_permission_id:
        link.image_permission_id = image_permission_id
        changed = True
    if link.notes != row.notes:
        link.notes = row.notes
        changed = True
    return changed


def link_fields_changed(
    link: ResourceImagePermissionModel,
    row: JournalPermissionRow,
    image_permission_id: Optional[int],
) -> bool:
    return any([
        image_permission_id is not None and link.image_permission_id != image_permission_id,
        link.notes != row.notes,
    ])


def find_resource_link(
    db: Session,
    resource_id: int,
    start_year: Optional[int],
    end_year: Optional[int],
) -> Optional[ResourceImagePermissionModel]:
    return db.query(ResourceImagePermissionModel).filter(
        ResourceImagePermissionModel.resource_id == resource_id,
        func.coalesce(ResourceImagePermissionModel.start_year, -1) == (start_year if start_year is not None else -1),
        func.coalesce(ResourceImagePermissionModel.end_year, -1) == (end_year if end_year is not None else -1),
    ).order_by(ResourceImagePermissionModel.resource_image_permission_id).first()


def add_failed_row(
    stats: LoadStats,
    row: JournalPermissionRow,
    reason: str,
    detail: str,
) -> None:
    stats.failed_rows.append(FailedRow(
        line_no=row.line_no,
        curator_id=row.curator_id,
        journal_abbreviation=row.journal_abbreviation,
        publisher=row.publisher,
        full_journal_name=row.full_journal_name,
        reason=reason,
        detail=detail,
    ))


def record_skipped_resource(
    stats: LoadStats,
    row: JournalPermissionRow,
    match_reason: str,
    detail: str,
) -> None:
    stats.rows_skipped += 1
    if "ambiguous" in match_reason:
        stats.resources_ambiguous += 1
    else:
        stats.resources_unmatched += 1
    add_failed_row(stats, row, match_reason, detail)
    logger.warning(
        f"Line {row.line_no}: skipped resource link ({match_reason}) for "
        f"{row.journal_abbreviation or row.full_journal_name}"
    )


def upsert_permission(
    db: Session,
    row: JournalPermissionRow,
    existing_permissions: Dict[str, ImagePermissionModel],
    stats: LoadStats,
    apply: bool,
) -> Optional[ImagePermissionModel]:
    permission = existing_permissions.get(row.permission_name)
    if permission is None:
        permission = existing_permissions.get(row.legacy_permission_name)
    if permission is None:
        stats.permissions_created += 1
        logger.info(f"Line {row.line_no}: create image_permission '{row.permission_name}'")
        if not apply:
            return None
        permission = ImagePermissionModel(
            name=row.permission_name,
            permission_text=row.permission_text,
            permission_url=row.permission_url,
            can_display_images=row.can_display_images,
        )
        db.add(permission)
        db.flush()
        existing_permissions[row.permission_name] = permission
        return permission

    changed = update_permission_fields(permission, row) if apply else permission_fields_changed(permission, row)
    if changed:
        stats.permissions_updated += 1
        logger.info(f"Line {row.line_no}: update image_permission '{row.permission_name}'")
        if apply:
            existing_permissions[row.permission_name] = permission
    else:
        stats.permissions_unchanged += 1
    return permission


def upsert_resource_link(
    db: Session,
    row: JournalPermissionRow,
    resource: ResourceModel,
    permission: Optional[ImagePermissionModel],
    stats: LoadStats,
    apply: bool,
) -> None:
    image_permission_id = permission.image_permission_id if permission is not None else None
    link = None
    if image_permission_id is not None:
        link = find_resource_link(
            db,
            resource.resource_id,
            row.start_year,
            row.end_year,
        )

    if link is None:
        stats.links_created += 1
        logger.info(
            f"Line {row.line_no}: create resource_image_permission "
            f"{resource.curie} -> '{row.permission_name}' ({range_label(row.start_year, row.end_year)})"
        )
        if apply:
            db.add(ResourceImagePermissionModel(
                resource_id=resource.resource_id,
                image_permission_id=image_permission_id,
                start_year=row.start_year,
                end_year=row.end_year,
                notes=row.notes,
            ))
        return

    changed = (
        update_link_fields(link, row, image_permission_id)
        if apply and image_permission_id is not None
        else link_fields_changed(link, row, image_permission_id)
    )
    if changed:
        stats.links_updated += 1
        logger.info(
            f"Line {row.line_no}: update resource_image_permission "
            f"{link.resource_image_permission_id}"
        )
    else:
        stats.links_unchanged += 1


def process_row(
    db: Session,
    row: JournalPermissionRow,
    resource: ResourceModel,
    existing_permissions: Dict[str, ImagePermissionModel],
    stats: LoadStats,
    apply: bool,
) -> None:
    savepoint = db.begin_nested() if apply else None
    try:
        permission = upsert_permission(db, row, existing_permissions, stats, apply)
        upsert_resource_link(db, row, resource, permission, stats, apply)
        if savepoint:
            savepoint.commit()
    except IntegrityError as err:
        if savepoint:
            savepoint.rollback()
        stats.errors += 1
        add_failed_row(stats, row, "integrity error", str(err.orig if hasattr(err, "orig") else err))
        logger.error(f"Line {row.line_no}: integrity error for {row.permission_name}: {err}")
    except Exception as err:
        if savepoint:
            savepoint.rollback()
        stats.errors += 1
        add_failed_row(stats, row, "error", str(err))
        logger.error(f"Line {row.line_no}: error for {row.permission_name}: {err}")


def load_permissions(
    db: Session,
    input_file: Path,
    manual_resource_map: Dict[str, str],
    apply: bool,
    subset_can_display: bool,
) -> LoadStats:
    stats = LoadStats()
    lookup = build_resource_lookup(db)
    existing_permissions = {
        permission.name: permission
        for permission in db.query(ImagePermissionModel).all()
    }

    for row in parse_tsv(input_file, subset_can_display):
        stats.rows_seen += 1
        resource, match_reason = find_resource(row, lookup, manual_resource_map)
        if resource is None:
            detail = describe_resource_match_failure(row, lookup, manual_resource_map, match_reason)
            record_skipped_resource(stats, row, match_reason, detail)
            continue
        process_row(db, row, resource, existing_permissions, stats, apply)

    if apply:
        db.commit()
    else:
        db.rollback()

    return stats


def log_summary(stats: LoadStats, apply: bool) -> None:
    mode = "APPLIED" if apply else "DRY RUN"
    logger.info("=" * 60)
    logger.info(f"{mode} summary")
    logger.info(f"Rows seen: {stats.rows_seen}")
    logger.info(f"Rows skipped: {stats.rows_skipped}")
    logger.info(f"Resource unmatched: {stats.resources_unmatched}")
    logger.info(f"Resource ambiguous: {stats.resources_ambiguous}")
    logger.info(f"Image permissions created: {stats.permissions_created}")
    logger.info(f"Image permissions updated: {stats.permissions_updated}")
    logger.info(f"Image permissions unchanged: {stats.permissions_unchanged}")
    logger.info(f"Resource links created: {stats.links_created}")
    logger.info(f"Resource links updated: {stats.links_updated}")
    logger.info(f"Resource links unchanged: {stats.links_unchanged}")
    logger.info(f"Errors: {stats.errors}")
    logger.info(f"Rows in failure report: {len(stats.failed_rows)}")
    logger.info("No rows are deleted by this loader.")


def write_failure_report(stats: LoadStats, report_file: Path) -> None:
    report_file.parent.mkdir(parents=True, exist_ok=True)
    with report_file.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            delimiter="\t",
            fieldnames=[
                "line_no",
                "curator_id",
                "journal_abbreviation",
                "publisher",
                "full_journal_name",
                "reason",
                "detail",
            ],
        )
        writer.writeheader()
        for row in stats.failed_rows:
            writer.writerow({
                "line_no": row.line_no,
                "curator_id": row.curator_id,
                "journal_abbreviation": row.journal_abbreviation,
                "publisher": row.publisher,
                "full_journal_name": row.full_journal_name,
                "reason": row.reason,
                "detail": row.detail,
            })
    logger.info(f"Wrote failure report: {report_file}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Upsert journal image permissions from journal_permission.tsv"
    )
    parser.add_argument(
        "--input-file",
        default=DEFAULT_INPUT_FILE,
        help=f"TSV input file; default: {DEFAULT_INPUT_FILE}",
    )
    parser.add_argument(
        "--env-file",
        default=None,
        help="Optional env file to load before connecting, e.g. .env_codex",
    )
    parser.add_argument(
        "--resource-map",
        default=None,
        help="Optional TSV with Journal (NLM abbrev) and resource_curie columns for manual matches",
    )
    parser.add_argument(
        "--report-file",
        default=DEFAULT_REPORT_FILE,
        help=f"TSV report for rows that could not be loaded; default: {DEFAULT_REPORT_FILE}",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Commit inserts/updates. Default is dry-run.",
    )
    parser.add_argument(
        "--subset-can-display",
        action="store_true",
        help="Treat 'Granted for a subset' style values as can_display_images=True.",
    )
    args = parser.parse_args()

    input_file = Path(args.input_file)
    env_file = Path(args.env_file) if args.env_file else None
    resource_map_file = Path(args.resource_map) if args.resource_map else None
    report_file = Path(args.report_file)

    if not input_file.exists():
        raise FileNotFoundError(f"Input file does not exist: {input_file}")

    load_env_file(env_file)
    manual_resource_map = read_manual_resource_map(resource_map_file)

    db = create_postgres_session(False)
    try:
        if args.apply:
            set_global_user_id(db, SCRIPT_USER)
        else:
            logger.info("Running in dry-run mode. Pass --apply to commit changes.")

        stats = load_permissions(
            db=db,
            input_file=input_file,
            manual_resource_map=manual_resource_map,
            apply=args.apply,
            subset_can_display=args.subset_can_display,
        )
        log_summary(stats, args.apply)
        write_failure_report(stats, report_file)
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    main()
