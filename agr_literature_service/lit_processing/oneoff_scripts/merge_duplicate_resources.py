"""
One-off script to merge duplicate resource records.

Duplicates are identified by exact title match (case-insensitive).
For each set of duplicates:
1. Choose canonical resource: the one with references attached, or older if both have refs
2. Move cross_references from duplicate to canonical
3. Move references from duplicate to canonical
4. Move editors from duplicate to canonical
5. Delete the duplicate resource

Usage:
    python merge_duplicate_resources.py

Run from within the development container or with proper environment setup.
"""
import logging
import sys
from typing import Dict, List, Tuple

from dotenv import load_dotenv
from sqlalchemy import func
from sqlalchemy.orm import Session

from agr_literature_service.api.models import (
    ResourceModel,
    ReferenceModel,
    CrossReferenceModel,
    EditorModel
)
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.user import set_global_user_id

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)


def has_only_mod_xrefs(db_session: Session, resource_id: int) -> bool:
    """
    Check if a resource has ONLY MOD-specific cross-references (FB:* or ZFIN:*)
    and no other identifier types (ISSN, NLM, etc.).

    These are the problematic resources that were created without sufficient
    identifiers to detect duplicates during ingestion.
    """
    mod_prefixes = ['FB', 'ZFIN']

    # Get all xref prefixes for this resource
    xref_prefixes = db_session.query(CrossReferenceModel.curie_prefix).filter(
        CrossReferenceModel.resource_id == resource_id
    ).distinct().all()

    if not xref_prefixes:
        return False

    # Check if ALL xrefs are MOD-specific
    for (prefix,) in xref_prefixes:
        if prefix not in mod_prefixes:
            return False

    return True


def find_duplicate_resources(db_session: Session) -> Dict[str, List[Tuple[int, str, int]]]:
    """
    Find resources with duplicate titles where at least one has ONLY MOD xrefs.

    This targets the specific case where:
    - FB created a resource with only FB:FBmultipub_* (no ISSN, no NLM)
    - ZFIN created a separate resource with ZFIN:* + NLM + ISSN

    We only merge when at least one resource has ONLY MOD xrefs (FB:* or ZFIN:*),
    indicating it was created without sufficient identifiers for duplicate detection.

    Returns:
        Dict mapping normalized title to list of (resource_id, curie, reference_count)

    Note:
        The query on func.lower(func.trim(ResourceModel.title)) will result in a
        sequential scan since there is no index on resource.title. This is acceptable
        for a one-off cleanup script but would need an index for production use.
    """
    # Find titles that have multiple resources
    duplicate_titles = db_session.query(
        func.lower(func.trim(ResourceModel.title)).label('norm_title'),
        func.count(ResourceModel.resource_id).label('cnt')
    ).filter(
        ResourceModel.title.isnot(None),
        ResourceModel.title != ''
    ).group_by(
        func.lower(func.trim(ResourceModel.title))
    ).having(
        func.count(ResourceModel.resource_id) > 1
    ).all()

    duplicates: Dict[str, List[Tuple[int, str, int]]] = {}

    for row in duplicate_titles:
        norm_title = row.norm_title
        # Get all resources with this title
        resources = db_session.query(
            ResourceModel.resource_id,
            ResourceModel.curie,
            ResourceModel.title
        ).filter(
            func.lower(func.trim(ResourceModel.title)) == norm_title
        ).order_by(
            ResourceModel.resource_id
        ).all()

        # Check if at least one resource has ONLY MOD xrefs (no ISSN, NLM, etc.)
        any_has_only_mod_xrefs = any(
            has_only_mod_xrefs(db_session, res.resource_id) for res in resources
        )
        if not any_has_only_mod_xrefs:
            # Skip - all resources have other identifiers, so they should have matched
            continue

        # Count references for each resource
        resource_info = []
        for res in resources:
            ref_count = db_session.query(func.count(ReferenceModel.reference_id)).filter(
                ReferenceModel.resource_id == res.resource_id
            ).scalar() or 0
            resource_info.append((res.resource_id, res.curie, ref_count))

        duplicates[norm_title] = resource_info

    return duplicates


def choose_canonical_resource(
    resource_info: List[Tuple[int, str, int]]
) -> Tuple[Tuple[int, str, int], List[Tuple[int, str, int]]]:
    """
    Choose which resource to keep as canonical.

    Priority:
    1. Resource with references attached
    2. If multiple have references (or none have), choose the older one (lower resource_id)

    Returns:
        Tuple of (canonical_resource, list_of_duplicates_to_merge)
    """
    # Sort by: has_references (desc), then resource_id (asc)
    sorted_resources = sorted(
        resource_info,
        key=lambda x: (-1 if x[2] > 0 else 0, x[0])
    )

    canonical = sorted_resources[0]
    duplicates = sorted_resources[1:]

    return canonical, duplicates


def merge_cross_references(
    db_session: Session,
    canonical_id: int,
    canonical_curie: str,
    duplicate_id: int
) -> int:
    """
    Move cross_references from duplicate resource to canonical resource.
    Skip if xref curie already exists on canonical.

    Returns:
        Number of xrefs moved
    """
    # Get existing xref curies on canonical
    canonical_xrefs = db_session.query(CrossReferenceModel.curie).filter(
        CrossReferenceModel.resource_id == canonical_id
    ).all()
    canonical_curie_set = {x.curie for x in canonical_xrefs}

    # Get xrefs from duplicate
    duplicate_xrefs = db_session.query(CrossReferenceModel).filter(
        CrossReferenceModel.resource_id == duplicate_id
    ).all()

    moved = 0
    for xref in duplicate_xrefs:
        if xref.curie in canonical_curie_set:
            # Already exists on canonical, delete the duplicate xref
            logger.info(f"  Deleting duplicate xref {xref.curie} (already on canonical)")
            db_session.delete(xref)
        else:
            # Move to canonical
            logger.info(f"  Moving xref {xref.curie} to {canonical_curie}")
            xref.resource_id = canonical_id
            canonical_curie_set.add(xref.curie)
            moved += 1

    return moved


def merge_references(
    db_session: Session,
    canonical_id: int,
    canonical_curie: str,
    duplicate_id: int
) -> int:
    """
    Move references from duplicate resource to canonical resource.

    Returns:
        Number of references moved
    """
    references = db_session.query(ReferenceModel).filter(
        ReferenceModel.resource_id == duplicate_id
    ).all()

    for ref in references:
        logger.info(f"  Moving reference {ref.curie} to {canonical_curie}")
        ref.resource_id = canonical_id

    return len(references)


def merge_editors(
    db_session: Session,
    canonical_id: int,
    canonical_curie: str,
    duplicate_id: int
) -> int:
    """
    Move editors from duplicate resource to canonical resource.
    Skip if editor with same name already exists on canonical.

    Returns:
        Number of editors moved
    """
    # Get existing editor names on canonical
    canonical_editors = db_session.query(EditorModel.name).filter(
        EditorModel.resource_id == canonical_id
    ).all()
    canonical_names = {e.name for e in canonical_editors if e.name}

    # Get editors from duplicate
    duplicate_editors = db_session.query(EditorModel).filter(
        EditorModel.resource_id == duplicate_id
    ).all()

    moved = 0
    for editor in duplicate_editors:
        if editor.name in canonical_names:
            # Already exists on canonical, delete the duplicate editor
            logger.info(f"  Deleting duplicate editor {editor.name} (already on canonical)")
            db_session.delete(editor)
        else:
            # Move to canonical
            logger.info(f"  Moving editor {editor.name} to {canonical_curie}")
            editor.resource_id = canonical_id
            if editor.name:
                canonical_names.add(editor.name)
            moved += 1

    return moved


def delete_duplicate_resource(db_session: Session, resource_id: int, curie: str) -> None:
    """
    Delete a duplicate resource after all its data has been moved.
    """
    resource = db_session.query(ResourceModel).filter(
        ResourceModel.resource_id == resource_id
    ).first()

    if resource:
        logger.info(f"  Deleting duplicate resource {curie} (id={resource_id})")
        db_session.delete(resource)


def merge_duplicate_resources(db_session: Session) -> Dict[str, int]:
    """
    Main function to find and merge all duplicate resources.

    Returns:
        Dict with statistics about the merge operation
    """
    stats = {
        'duplicate_sets': 0,
        'resources_merged': 0,
        'xrefs_moved': 0,
        'references_moved': 0,
        'editors_moved': 0,
        'resources_deleted': 0
    }

    logger.info("Finding duplicate resources...")
    duplicates = find_duplicate_resources(db_session)
    stats['duplicate_sets'] = len(duplicates)

    if not duplicates:
        logger.info("No duplicate resources found.")
        return stats

    logger.info(f"Found {len(duplicates)} sets of duplicate resources")

    for title, resource_info in duplicates.items():
        logger.info(f"\nProcessing duplicates for: '{title}'")
        logger.info(f"  Resources: {resource_info}")

        canonical, to_merge = choose_canonical_resource(resource_info)
        canonical_id, canonical_curie, canonical_refs = canonical

        logger.info(f"  Canonical: {canonical_curie} (id={canonical_id}, refs={canonical_refs})")

        for dup_id, dup_curie, dup_refs in to_merge:
            logger.info(f"  Merging: {dup_curie} (id={dup_id}, refs={dup_refs})")
            stats['resources_merged'] += 1

            # Move cross_references
            xrefs_moved = merge_cross_references(
                db_session, canonical_id, canonical_curie, dup_id
            )
            stats['xrefs_moved'] += xrefs_moved

            # Move references
            refs_moved = merge_references(
                db_session, canonical_id, canonical_curie, dup_id
            )
            stats['references_moved'] += refs_moved

            # Move editors
            editors_moved = merge_editors(
                db_session, canonical_id, canonical_curie, dup_id
            )
            stats['editors_moved'] += editors_moved

            # Delete the duplicate resource
            delete_duplicate_resource(db_session, dup_id, dup_curie)
            stats['resources_deleted'] += 1

        # Commit after each duplicate set
        db_session.commit()
        logger.info(f"  Committed merge for '{title}'")

    return stats


def main():
    """Main entry point."""
    logger.info("Starting duplicate resource merge script")

    db_session = create_postgres_session(False)
    set_global_user_id(db_session, "merge_duplicate_resources")

    try:
        stats = merge_duplicate_resources(db_session)

        logger.info("\n" + "=" * 60)
        logger.info("MERGE COMPLETE - Statistics:")
        logger.info(f"  Duplicate sets found: {stats['duplicate_sets']}")
        logger.info(f"  Resources merged: {stats['resources_merged']}")
        logger.info(f"  Cross-references moved: {stats['xrefs_moved']}")
        logger.info(f"  References moved: {stats['references_moved']}")
        logger.info(f"  Editors moved: {stats['editors_moved']}")
        logger.info(f"  Resources deleted: {stats['resources_deleted']}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Error during merge: {e}")
        db_session.rollback()
        raise
    finally:
        db_session.close()

    logger.info("Duplicate resource merge script completed")


if __name__ == "__main__":
    main()
