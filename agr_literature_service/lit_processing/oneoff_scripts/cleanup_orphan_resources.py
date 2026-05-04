"""
Script to clean up orphan resources in the resource table.

Orphan resources are those where resource_id is NOT in:
- cross_reference table (resource_id column)
- reference table (resource_id column)

Before removing an orphan resource:
1. Check if its print_issn or online_issn (as ISSN:xxxx-xxxx curie) is associated
   with another resource_id in cross_reference table
2. If yes, merge title_synonyms and abbreviation_synonyms from the orphan
   into the other resource
3. Then delete the orphan resource
"""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Set, Tuple

from sqlalchemy import text

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logging.basicConfig(format='%(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@dataclass
class OrphanResource:
    resource_id: int
    curie: str | None
    title: str | None
    print_issn: str | None
    online_issn: str | None
    title_synonyms: List[str]
    abbreviation_synonyms: List[str]
    merge_target_id: int | None = None
    merge_via_issn: str | None = None


@dataclass
class MergeAction:
    from_resource_id: int
    to_resource_id: int
    via_issn: str
    title_synonyms_to_add: List[str] = field(default_factory=list)
    abbreviation_synonyms_to_add: List[str] = field(default_factory=list)


def _write_file(path: str, content: str) -> None:
    with open(path, "w") as f:
        f.write(content)


def find_orphan_resources(db) -> List[OrphanResource]:
    """
    Find resources that are not referenced by cross_reference or reference tables.
    """
    sql = text("""
        SELECT
            r.resource_id,
            r.curie,
            r.title,
            r.print_issn,
            r.online_issn,
            r.title_synonyms,
            r.abbreviation_synonyms
        FROM resource r
        WHERE NOT EXISTS (
            SELECT 1 FROM cross_reference cr WHERE cr.resource_id = r.resource_id
        )
        AND NOT EXISTS (
            SELECT 1 FROM reference ref WHERE ref.resource_id = r.resource_id
        )
        ORDER BY r.resource_id
    """)

    rows = db.execute(sql).fetchall()

    orphans = []
    for row in rows:
        resource_id, curie, title, print_issn, online_issn, title_syns, abbrev_syns = row
        orphans.append(OrphanResource(
            resource_id=resource_id,
            curie=curie,
            title=title,
            print_issn=print_issn,
            online_issn=online_issn,
            title_synonyms=list(title_syns) if title_syns else [],
            abbreviation_synonyms=list(abbrev_syns) if abbrev_syns else []
        ))

    return orphans


def find_issn_to_resource_mapping(db) -> Dict[str, int]:
    """
    Build a mapping from ISSN curie to resource_id from cross_reference table.
    Only includes non-obsolete ISSN curies.
    """
    sql = text("""
        SELECT DISTINCT cr.curie, cr.resource_id
        FROM cross_reference cr
        WHERE cr.resource_id IS NOT NULL
          AND cr.is_obsolete = false
          AND cr.curie_prefix = 'ISSN'
        ORDER BY cr.curie
    """)

    rows = db.execute(sql).fetchall()

    mapping: Dict[str, int] = {}
    for curie, resource_id in rows:
        # If same ISSN maps to multiple resources, keep the first one
        if curie not in mapping:
            mapping[curie] = resource_id

    return mapping


def find_merge_targets(
    orphans: List[OrphanResource],
    issn_to_resource: Dict[str, int]
) -> Tuple[List[OrphanResource], List[MergeAction]]:
    """
    For each orphan, check if its print_issn or online_issn maps to another resource.
    Returns updated orphans with merge_target_id set, and list of merge actions.
    """
    merge_actions: List[MergeAction] = []

    for orphan in orphans:
        # Build ISSN curies from print_issn and online_issn
        issn_curies = []
        if orphan.print_issn:
            issn_curies.append(f"ISSN:{orphan.print_issn}")
        if orphan.online_issn:
            issn_curies.append(f"ISSN:{orphan.online_issn}")

        # Check if any ISSN maps to another resource
        for issn_curie in issn_curies:
            if issn_curie in issn_to_resource:
                target_id = issn_to_resource[issn_curie]
                if target_id != orphan.resource_id:
                    orphan.merge_target_id = target_id
                    orphan.merge_via_issn = issn_curie

                    merge_actions.append(MergeAction(
                        from_resource_id=orphan.resource_id,
                        to_resource_id=target_id,
                        via_issn=issn_curie,
                        title_synonyms_to_add=orphan.title_synonyms,
                        abbreviation_synonyms_to_add=orphan.abbreviation_synonyms
                    ))
                    break  # Only need one match

    return orphans, merge_actions


def get_existing_synonyms(db, resource_ids: Set[int]) -> Dict[int, Tuple[List[str], List[str]]]:
    """
    Get existing title_synonyms and abbreviation_synonyms for given resource_ids.
    Returns dict mapping resource_id -> (title_synonyms, abbreviation_synonyms)
    """
    if not resource_ids:
        return {}

    sql = text("""
        SELECT resource_id, title_synonyms, abbreviation_synonyms
        FROM resource
        WHERE resource_id = ANY(:ids)
    """)

    rows = db.execute(sql, {"ids": list(resource_ids)}).fetchall()

    result: Dict[int, Tuple[List[str], List[str]]] = {}
    for resource_id, title_syns, abbrev_syns in rows:
        result[resource_id] = (
            list(title_syns) if title_syns else [],
            list(abbrev_syns) if abbrev_syns else []
        )

    return result


def apply_changes(
    db,
    merge_actions: List[MergeAction],
    orphan_ids: List[int],
    existing_synonyms: Dict[int, Tuple[List[str], List[str]]]
) -> Tuple[int, int]:
    """
    Apply merge actions and delete orphan resources.
    Returns (num_merged, num_deleted)
    """
    num_merged = 0
    num_deleted = 0

    # Apply merges
    update_sql = text("""
        UPDATE resource
        SET title_synonyms = :title_syns,
            abbreviation_synonyms = :abbrev_syns
        WHERE resource_id = :resource_id
    """)

    for action in merge_actions:
        existing_title, existing_abbrev = existing_synonyms.get(
            action.to_resource_id, ([], [])
        )

        # Merge synonyms (avoid duplicates)
        new_title_syns = list(set(existing_title + action.title_synonyms_to_add))
        new_abbrev_syns = list(set(existing_abbrev + action.abbreviation_synonyms_to_add))

        # Only update if there's something to add
        if action.title_synonyms_to_add or action.abbreviation_synonyms_to_add:
            db.execute(update_sql, {
                "resource_id": action.to_resource_id,
                "title_syns": new_title_syns if new_title_syns else None,
                "abbrev_syns": new_abbrev_syns if new_abbrev_syns else None
            })
            num_merged += 1
            logger.info(
                f"  Merged synonyms: resource {action.from_resource_id} -> "
                f"{action.to_resource_id} via {action.via_issn}"
            )

    # Delete orphan resources
    delete_sql = text("DELETE FROM resource WHERE resource_id = :rid")

    for rid in orphan_ids:
        db.execute(delete_sql, {"rid": rid})
        num_deleted += 1

    db.commit()
    return num_merged, num_deleted


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Clean up orphan resources in the resource table"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not modify DB; only report actions"
    )
    parser.add_argument(
        "--out-prefix",
        default="orphan_resources",
        help="Prefix for output files"
    )
    args = parser.parse_args()

    db = create_postgres_session(False)

    try:
        # Find orphan resources
        logger.info("Finding orphan resources...")
        orphans = find_orphan_resources(db)

        if not orphans:
            logger.info("No orphan resources found.")
            _write_file(f"{args.out_prefix}_summary.txt", "No orphan resources found.\n")
            return

        logger.info(f"Found {len(orphans)} orphan resources")

        # Build ISSN to resource mapping
        logger.info("Building ISSN to resource mapping...")
        issn_to_resource = find_issn_to_resource_mapping(db)
        logger.info(f"Found {len(issn_to_resource)} ISSN mappings")

        # Find merge targets
        logger.info("Finding merge targets...")
        orphans, merge_actions = find_merge_targets(orphans, issn_to_resource)

        orphans_with_merge = [o for o in orphans if o.merge_target_id is not None]
        orphans_without_merge = [o for o in orphans if o.merge_target_id is None]

        logger.info(f"  {len(orphans_with_merge)} orphans will merge synonyms before deletion")
        logger.info(f"  {len(orphans_without_merge)} orphans will be deleted directly")

        # Get existing synonyms for merge targets
        target_ids = {a.to_resource_id for a in merge_actions}
        existing_synonyms = get_existing_synonyms(db, target_ids)

        # Build summary
        summary_lines = [
            "=" * 100,
            "ORPHAN RESOURCES CLEANUP SUMMARY",
            "=" * 100,
            f"Dry run: {args.dry_run}",
            "",
            f"Total orphan resources found: {len(orphans)}",
            f"  - With merge target (ISSN match): {len(orphans_with_merge)}",
            f"  - Without merge target: {len(orphans_without_merge)}",
            "",
            f"Total merge actions: {len(merge_actions)}",
            f"Total resources to delete: {len(orphans)}",
            "",
        ]

        if merge_actions:
            summary_lines.append("MERGE ACTIONS:")
            for action in merge_actions:
                summary_lines.append(
                    f"  Resource {action.from_resource_id} -> {action.to_resource_id} "
                    f"via {action.via_issn}"
                )
                if action.title_synonyms_to_add:
                    summary_lines.append(
                        f"    title_synonyms to add: {action.title_synonyms_to_add}"
                    )
                if action.abbreviation_synonyms_to_add:
                    summary_lines.append(
                        f"    abbreviation_synonyms to add: {action.abbreviation_synonyms_to_add}"
                    )
            summary_lines.append("")

        summary_lines.append("RESOURCES TO DELETE:")
        for orphan in orphans:
            merge_info = ""
            if orphan.merge_target_id:
                merge_info = f" (after merging to {orphan.merge_target_id})"
            summary_lines.append(
                f"  resource_id={orphan.resource_id}, curie={orphan.curie}, "
                f"title={orphan.title[:50] if orphan.title else None}...{merge_info}"
            )
        summary_lines.append("")

        _write_file(f"{args.out_prefix}_summary.txt", "\n".join(summary_lines))
        logger.info(f"Wrote: {args.out_prefix}_summary.txt")

        # Write detailed report
        detail_lines = [
            "=" * 100,
            "ORPHAN RESOURCES - DETAILED REPORT",
            "=" * 100,
            "",
        ]

        for orphan in orphans:
            detail_lines.append("-" * 100)
            detail_lines.append(f"Resource ID: {orphan.resource_id}")
            detail_lines.append(f"  CURIE: {orphan.curie}")
            detail_lines.append(f"  Title: {orphan.title}")
            detail_lines.append(f"  Print ISSN: {orphan.print_issn}")
            detail_lines.append(f"  Online ISSN: {orphan.online_issn}")
            detail_lines.append(f"  Title Synonyms: {orphan.title_synonyms}")
            detail_lines.append(f"  Abbreviation Synonyms: {orphan.abbreviation_synonyms}")
            if orphan.merge_target_id:
                detail_lines.append(f"  MERGE TARGET: {orphan.merge_target_id}")
                detail_lines.append(f"  MERGE VIA ISSN: {orphan.merge_via_issn}")
            else:
                detail_lines.append("  MERGE TARGET: None (direct delete)")
            detail_lines.append("")

        _write_file(f"{args.out_prefix}_details.txt", "\n".join(detail_lines))
        logger.info(f"Wrote: {args.out_prefix}_details.txt")

        # Apply changes
        if not args.dry_run:
            logger.info("Applying changes to database...")
            orphan_ids = [o.resource_id for o in orphans]
            num_merged, num_deleted = apply_changes(
                db, merge_actions, orphan_ids, existing_synonyms
            )
            logger.info(
                f"Committed: {num_merged} synonym merges, {num_deleted} resources deleted"
            )
        else:
            logger.info("Dry run complete (no DB changes).")

    except Exception as e:
        logger.error(f"Error: {e}")
        try:
            db.rollback()
        except Exception:
            pass
        raise
    finally:
        db.close()


if __name__ == "__main__":
    main()
