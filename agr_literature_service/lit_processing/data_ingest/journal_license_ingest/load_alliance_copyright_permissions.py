#!/usr/bin/env python3
"""
Load the image working group's publisher copyright/permission sheet into
image_permission and resource_image_permission (SCRUM-6416).

Input is data/alliance_copyright_permissions.tsv, a curated per-(journal,
grant) export of the working group's "Copyright licence statements" Google
sheet. Rows marked skip=yes (publishers whose statement is still being decided)
are reported and never loaded, so the seed file can stay complete while the
working group finishes; flipping the skip cell makes a row loadable.

Differences from load_journal_image_permissions.py, which this reuses helpers
from: rows are journal-title keyed (no NLM abbreviation column), a publisher
synonym list sanity-checks the matched resource's publisher field (mismatch is
a warning, not a failure: the grant is per journal), and one journal can carry
several grants distinguished by year range AND permission (the two SfN 2026-
grants share a range and differ only by OA status), so links are matched on
(resource, permission, start, end), not (resource, range).

Like the other loaders here this is non-destructive and dry-run by default:
    python load_alliance_copyright_permissions.py            # dry run
    python load_alliance_copyright_permissions.py --apply
"""

import argparse
import csv
import logging
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Tuple

from sqlalchemy.orm import Session

from agr_literature_service.api.models import (
    ImagePermissionModel,
    ResourceImagePermissionModel,
    ResourceModel,
)
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.lit_processing.data_ingest.journal_license_ingest.load_journal_image_permissions import (
    ResourceLookup,
    build_resource_lookup,
    choose_active_resource,
    choose_unique,
    clean,
    normalize_exact,
    normalize_for_match,
    range_label,
)

logging.basicConfig(format="%(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DEFAULT_INPUT_FILE = "data/alliance_copyright_permissions.tsv"
SCRIPT_USER = "load_alliance_copyright_permissions"


@dataclass
class GrantRow:
    line_no: int
    publisher: str
    publisher_synonyms: List[str]
    journal_title: str
    permission_name: str
    permission_type: str
    permission_text: str
    permission_url: Optional[str]
    can_display_images: bool
    start_year: Optional[int]
    end_year: Optional[int]
    notes: Optional[str]


@dataclass
class Stats:
    rows_seen: int = 0
    rows_skipped: int = 0
    resources_unmatched: int = 0
    publisher_mismatches: int = 0
    permissions_created: int = 0
    permissions_updated: int = 0
    permissions_unchanged: int = 0
    links_created: int = 0
    links_updated: int = 0
    links_unchanged: int = 0
    problems: List[str] = field(default_factory=list)


def parse_year(value: str) -> Optional[int]:
    value = clean(value)
    return int(value) if value else None


def parse_grant_rows(reader: Iterable[Dict[str, str]]) -> Tuple[List[GrantRow], List[str]]:
    """Return (loadable rows, skip messages). Raises ValueError on malformed rows
    so a mangled export fails loudly instead of loading garbage."""
    rows: List[GrantRow] = []
    skipped: List[str] = []
    for line_no, raw in enumerate(reader, start=2):
        get = lambda key: clean(raw.get(key) or "")  # noqa: E731
        publisher = get("publisher")
        journal = get("journal_title")
        if not publisher and not journal:
            continue
        if get("skip") == "yes":
            skipped.append(f"line {line_no}: SKIP {publisher} / {journal}: {get('skip_reason')}")
            continue
        name = get("permission_name")
        text = get("attribution_text")
        can_display = get("can_display_images")
        if not (publisher and journal and name and text) or can_display not in ("yes", "no"):
            raise ValueError(f"line {line_no}: malformed row for {publisher!r} / {journal!r}")
        rows.append(GrantRow(
            line_no=line_no,
            publisher=publisher,
            publisher_synonyms=[s for s in (clean(x) for x in get("publisher_synonyms").split("|")) if s],
            journal_title=journal,
            permission_name=name,
            permission_type=get("permission_type"),
            permission_text=text,
            permission_url=None,
            can_display_images=(can_display == "yes"),
            start_year=parse_year(get("start_year")),
            end_year=parse_year(get("end_year")),
            notes=get("link_notes") or None,
        ))
    return rows, skipped


def find_resource_by_title(journal_title: str, lookup: ResourceLookup) -> Optional[ResourceModel]:
    """Match a journal title against resource titles and abbreviations, exact
    first, then normalized; unique match wins, else unique match among
    resources that actually have references."""
    for candidates in (
        lookup.resource_by_exact_title.get(normalize_exact(journal_title), []),
        lookup.resource_by_exact_abbreviation.get(normalize_exact(journal_title), []),
        lookup.resource_by_title.get(normalize_for_match(journal_title), []),
        lookup.resource_by_abbreviation.get(normalize_for_match(journal_title), []),
    ):
        resource = choose_unique(candidates) or choose_active_resource(candidates, lookup)
        if resource:
            return resource
    return None


def publisher_matches(resource: ResourceModel, synonyms: List[str]) -> bool:
    resource_publisher = normalize_for_match(resource.publisher or "")
    if not resource_publisher:
        return True  # nothing to check against
    return any(normalize_for_match(s) == resource_publisher for s in synonyms)


def upsert_permission(db: Session, row: GrantRow,
                      existing: Dict[str, ImagePermissionModel],
                      stats: Stats, apply: bool) -> Optional[ImagePermissionModel]:
    permission = existing.get(row.permission_name)
    if permission is None:
        stats.permissions_created += 1
        logger.info(f"line {row.line_no}: create image_permission '{row.permission_name}'")
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
        existing[row.permission_name] = permission
        return permission
    changed = (permission.permission_text != row.permission_text
               or permission.can_display_images != row.can_display_images)
    if changed:
        stats.permissions_updated += 1
        logger.info(f"line {row.line_no}: update image_permission '{row.permission_name}'")
        if apply:
            permission.permission_text = row.permission_text
            permission.can_display_images = row.can_display_images
    else:
        stats.permissions_unchanged += 1
    return permission


def upsert_link(db: Session, row: GrantRow, resource: ResourceModel,
                permission: Optional[ImagePermissionModel],
                stats: Stats, apply: bool) -> None:
    label = f"{resource.curie} ({row.journal_title}) -> '{row.permission_name}' ({range_label(row.start_year, row.end_year)})"
    link = None
    if permission is not None:
        link = (
            db.query(ResourceImagePermissionModel)
            .filter_by(resource_id=resource.resource_id,
                       image_permission_id=permission.image_permission_id,
                       start_year=row.start_year,
                       end_year=row.end_year)
            .one_or_none()
        )
    if link is None:
        stats.links_created += 1
        logger.info(f"line {row.line_no}: create link {label}")
        if apply and permission is not None:
            db.add(ResourceImagePermissionModel(
                resource_id=resource.resource_id,
                image_permission_id=permission.image_permission_id,
                start_year=row.start_year,
                end_year=row.end_year,
                notes=row.notes,
            ))
        return
    if (link.notes or None) != row.notes:
        stats.links_updated += 1
        logger.info(f"line {row.line_no}: update link notes {label}")
        if apply:
            link.notes = row.notes
    else:
        stats.links_unchanged += 1


def load(db: Session, input_file: str, apply: bool) -> Stats:
    stats = Stats()
    with open(input_file, newline="") as fh:
        rows, skipped = parse_grant_rows(csv.DictReader(fh, delimiter="\t"))
    for message in skipped:
        logger.info(message)
    stats.rows_skipped = len(skipped)
    stats.rows_seen = len(rows) + len(skipped)

    lookup = build_resource_lookup(db)
    existing = {p.name: p for p in db.query(ImagePermissionModel).all()}

    for row in rows:
        resource = find_resource_by_title(row.journal_title, lookup)
        if resource is None:
            stats.resources_unmatched += 1
            stats.problems.append(
                f"line {row.line_no}: no unique resource for journal '{row.journal_title}' ({row.publisher})")
            continue
        if not publisher_matches(resource, row.publisher_synonyms):
            stats.publisher_mismatches += 1
            stats.problems.append(
                f"line {row.line_no}: {resource.curie} publisher {resource.publisher!r} not in the "
                f"{row.publisher} synonym list (loaded anyway; grant is per journal)")
        permission = upsert_permission(db, row, existing, stats, apply)
        upsert_link(db, row, resource, permission, stats, apply)

    if apply:
        db.commit()
    else:
        db.rollback()
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--input-file", default=DEFAULT_INPUT_FILE)
    parser.add_argument("--apply", action="store_true", help="commit changes (default is dry run)")
    args = parser.parse_args()

    db = create_postgres_session(False)
    set_global_user_id(db, SCRIPT_USER)
    try:
        stats = load(db, args.input_file, args.apply)
    finally:
        db.close()

    mode = "APPLY" if args.apply else "DRY RUN"
    logger.info(
        f"[{mode}] rows={stats.rows_seen} skipped={stats.rows_skipped} "
        f"unmatched={stats.resources_unmatched} publisher_mismatches={stats.publisher_mismatches} "
        f"permissions +{stats.permissions_created}/~{stats.permissions_updated}/={stats.permissions_unchanged} "
        f"links +{stats.links_created}/~{stats.links_updated}/={stats.links_unchanged}")
    for problem in stats.problems:
        logger.warning(problem)


if __name__ == "__main__":
    main()
