"""
Script to clean up duplicate resource curies in the cross_reference table.

Problem: Same curie (e.g., NLM:12345) can be associated with multiple resource_ids
via different cross_reference rows.

Solution:
1. Find all curies in cross_reference where resource_id IS NOT NULL
2. Identify curies that map to multiple different resource_ids
3. For each duplicate curie:
   - Determine which resource_id has the most references (COUNT in reference table)
   - Keep that resource_id's cross_reference entry (the "canonical" one)
   - Update all references pointing to losing resource_ids to point to the canonical resource_id
   - Delete the duplicate cross_reference entries for losing resource_ids
"""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from typing import Dict, List, Tuple

from sqlalchemy import text

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logging.basicConfig(format='%(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@dataclass
class ResourceInfo:
    resource_id: int
    resource_curie: str | None
    reference_count: int
    cross_reference_ids: List[int]


@dataclass
class DuplicateCurie:
    curie: str
    resources: List[ResourceInfo]
    canonical_resource_id: int


def _write_file(path: str, content: str) -> None:
    with open(path, "w") as f:
        f.write(content)


def find_duplicate_curies(db) -> Dict[str, List[Tuple[int, int, str | None, List[int]]]]:
    """
    Find curies in cross_reference that are associated with multiple resource_ids.

    Returns a dict mapping curie -> list of (resource_id, reference_count, resource_curie, xref_ids)
    """
    # Find all curies that have multiple resource_ids
    dup_sql = text("""
        WITH curie_resources AS (
            SELECT
                cr.curie,
                cr.resource_id,
                ARRAY_AGG(cr.cross_reference_id) AS xref_ids,
                COUNT(DISTINCT ref.reference_id) AS reference_count,
                res.curie AS resource_curie
            FROM cross_reference cr
            JOIN resource res ON res.resource_id = cr.resource_id
            LEFT JOIN reference ref ON ref.resource_id = cr.resource_id
            WHERE cr.resource_id IS NOT NULL
              AND cr.is_obsolete = false
            GROUP BY cr.curie, cr.resource_id, res.curie
        ),
        duplicate_curies AS (
            SELECT curie
            FROM curie_resources
            GROUP BY curie
            HAVING COUNT(DISTINCT resource_id) > 1
        )
        SELECT
            cr.curie,
            cr.resource_id,
            cr.reference_count,
            cr.resource_curie,
            cr.xref_ids
        FROM curie_resources cr
        JOIN duplicate_curies dc ON dc.curie = cr.curie
        ORDER BY cr.curie, cr.reference_count DESC, cr.resource_id ASC
    """)

    rows = db.execute(dup_sql).fetchall()

    duplicates: Dict[str, List[Tuple[int, int, str | None, List[int]]]] = {}
    for curie, resource_id, ref_count, resource_curie, xref_ids in rows:
        if curie not in duplicates:
            duplicates[curie] = []
        duplicates[curie].append((resource_id, ref_count, resource_curie, list(xref_ids)))

    return duplicates


def determine_canonical(
    resources: List[Tuple[int, int, str | None, List[int]]]
) -> Tuple[int, List[Tuple[int, int, str | None, List[int]]]]:
    """
    Determine the canonical resource_id (the one with most references).
    Returns (canonical_resource_id, list_of_non_canonical_resources)
    """
    # Sort by reference_count DESC, then resource_id ASC (for tie-breaking)
    sorted_resources = sorted(resources, key=lambda x: (-x[1], x[0]))
    canonical = sorted_resources[0]
    non_canonical = sorted_resources[1:]
    return canonical[0], non_canonical


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Clean up duplicate resource curies in cross_reference table"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not modify DB; only report actions"
    )
    parser.add_argument(
        "--out-prefix",
        default="duplicate_resource_curies",
        help="Prefix for output files"
    )
    args = parser.parse_args()

    db = create_postgres_session(False)

    try:
        logger.info("Finding duplicate curies in cross_reference table...")
        duplicates = find_duplicate_curies(db)

        if not duplicates:
            logger.info("No duplicate curies found.")
            _write_file(f"{args.out_prefix}_summary.txt", "No duplicate curies found.\n")
            return

        logger.info(f"Found {len(duplicates)} curies with multiple resource_ids")

        # Track actions
        actions: List[str] = []
        reference_updates: List[Tuple[int, int, int]] = []  # (from_resource, to_resource, count)
        xref_deletes: List[Tuple[str, int, List[int]]] = []  # (curie, resource_id, xref_ids)
        duplicate_details: List[DuplicateCurie] = []

        for curie, resources in duplicates.items():
            canonical_id, non_canonical = determine_canonical(resources)

            # Build resource info for reporting
            resource_infos = []
            for resource_id, ref_count, resource_curie, xref_ids in resources:
                resource_infos.append(ResourceInfo(
                    resource_id=resource_id,
                    resource_curie=resource_curie,
                    reference_count=ref_count,
                    cross_reference_ids=xref_ids
                ))

            duplicate_details.append(DuplicateCurie(
                curie=curie,
                resources=resource_infos,
                canonical_resource_id=canonical_id
            ))

            # Get canonical resource's reference count for logging
            canonical_ref_count = next(
                r[1] for r in resources if r[0] == canonical_id
            )

            actions.append(
                f"CURIE: {curie} -> canonical resource_id={canonical_id} "
                f"(refs={canonical_ref_count})"
            )

            for resource_id, ref_count, resource_curie, xref_ids in non_canonical:
                if ref_count > 0:
                    # Need to move references from this resource to canonical
                    reference_updates.append((resource_id, canonical_id, ref_count))
                    actions.append(
                        f"  UPDATE {ref_count} references: resource_id {resource_id} -> {canonical_id}"
                    )

                # Delete the duplicate cross_reference entries
                xref_deletes.append((curie, resource_id, xref_ids))
                actions.append(
                    f"  DELETE cross_reference: curie={curie}, resource_id={resource_id}, "
                    f"xref_ids={xref_ids}"
                )

        # Write summary
        summary_lines = [
            "=" * 100,
            "DUPLICATE RESOURCE CURIES IN CROSS_REFERENCE - CLEANUP SUMMARY",
            "=" * 100,
            f"Dry run: {args.dry_run}",
            f"Total duplicate curies found: {len(duplicates)}",
            f"Total reference updates planned: {sum(u[2] for u in reference_updates)}",
            f"Total cross_reference deletes planned: {sum(len(d[2]) for d in xref_deletes)}",
            "",
            "Actions:",
        ]
        summary_lines.extend(f"  {a}" for a in actions)
        summary_lines.append("")

        _write_file(f"{args.out_prefix}_summary.txt", "\n".join(summary_lines))
        logger.info(f"Wrote: {args.out_prefix}_summary.txt")

        # Write detailed report
        detail_lines = [
            "=" * 100,
            "DUPLICATE RESOURCE CURIES - DETAILED REPORT",
            "=" * 100,
            "",
        ]

        for dup in duplicate_details:
            detail_lines.append("-" * 100)
            detail_lines.append(f"CURIE: {dup.curie}")
            detail_lines.append(f"Canonical resource_id: {dup.canonical_resource_id}")
            detail_lines.append("")
            for res in dup.resources:
                is_canonical = "(CANONICAL)" if res.resource_id == dup.canonical_resource_id else ""
                detail_lines.append(f"  Resource ID: {res.resource_id} {is_canonical}")
                detail_lines.append(f"    Resource CURIE: {res.resource_curie}")
                detail_lines.append(f"    Reference Count: {res.reference_count}")
                detail_lines.append(f"    Cross Reference IDs: {res.cross_reference_ids}")
                detail_lines.append("")
            detail_lines.append("")

        _write_file(f"{args.out_prefix}_details.txt", "\n".join(detail_lines))
        logger.info(f"Wrote: {args.out_prefix}_details.txt")

        # Apply changes if not dry-run
        if not args.dry_run:
            logger.info("Applying changes to database...")

            # Update references to point to canonical resource
            update_ref_sql = text("""
                UPDATE reference
                SET resource_id = :to_id
                WHERE resource_id = :from_id
            """)

            total_refs_updated = 0
            for from_id, to_id, count in reference_updates:
                db.execute(update_ref_sql, {"from_id": from_id, "to_id": to_id})
                total_refs_updated += count
                logger.info(f"  Updated references: {from_id} -> {to_id} ({count} refs)")

            # Delete duplicate cross_reference entries
            delete_xref_sql = text("""
                DELETE FROM cross_reference
                WHERE cross_reference_id = ANY(:xref_ids)
            """)

            total_xrefs_deleted = 0
            for curie, resource_id, xref_ids in xref_deletes:
                db.execute(delete_xref_sql, {"xref_ids": xref_ids})
                total_xrefs_deleted += len(xref_ids)
                logger.info(f"  Deleted cross_references: curie={curie}, resource_id={resource_id}")

            db.commit()
            logger.info(
                f"Committed: {total_refs_updated} reference updates, "
                f"{total_xrefs_deleted} cross_reference deletes"
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
