#!/usr/bin/env python3
"""
Delete every image permission loaded from the old image-wg spreadsheet
(SCRUM-6587).

The image working group decided the old per-journal curator sheet
(journal_permission.tsv, loaded by load_journal_image_permissions.py under
SCRUM-6420) was not a valid basis for display permissions: the Alliance only
has permission from the publishers in the new copyright sheet (loaded by
load_alliance_copyright_permissions.py under SCRUM-6416). In particular there
is NO permission from Elsevier, yet the old load minted 16 Elsevier grants.

So this script removes the old load wholesale: every image_permission row
created by load_journal_image_permissions and its resource_image_permission
links. Rows are selected by created_by, not by name: the spelling fixes of
2026-09-18 renamed some rows (updated_by fix_image_permission_spellings_and_
attribution) but left created_by intact, and created_by is stable across
dev/stage/prod while ids drift. The new-sheet grants (created_by
load_alliance_copyright_permissions) are untouched.

One stray is named explicitly: 'WormBook - Permission granted Alliance'
(created the same day as the old load but before user stamping, so its
created_by is NULL; zero links on dev).

Safety: a permission carrying a link some OTHER user created is a curator
decision, not cleanup — it is reported as a problem, and --apply commits
nothing while problems exist (dev shows zero such links). Deletes go through
the ORM session so the version/audit tables record them.

The resource_image_permission delete triggers refresh the denormalized
reference.can_display_image column, and those triggers use the
compute_can_display_image function installed at API startup — as with the
loaders (see journal_license_ingest/README.md), restart the API on current
code BEFORE running with --apply, or recompute the affected resources
afterwards with refresh_can_display_image_for_resource(resource_id).

    python delete_old_image_spreadsheet_permissions.py          # dry run
    python delete_old_image_spreadsheet_permissions.py --apply
"""

import argparse
import logging
from os import path

from agr_literature_service.api.models import (
    ImagePermissionModel,
    ResourceImagePermissionModel,
    ResourceModel,
)
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logging.basicConfig(format="%(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

SCRIPT_USER = path.basename(__file__).replace(".py", "")

OLD_LOADER_USER = "load_journal_image_permissions"

# updated_by values that do not indicate a manual edit: the loader itself and
# the SCRUM-6587-adjacent spelling/attribution fix. Anything else is only
# logged (updates do not change whose data it is), unlike a foreign
# created_by on a link, which blocks the delete.
EXPECTED_EDITORS = {OLD_LOADER_USER, "fix_image_permission_spellings_and_attribution"}

# Old-sheet rows the created_by query cannot see (see module docstring).
EXTRA_OLD_SHEET_PERMISSION_NAMES = ["WormBook - Permission granted Alliance"]


def delete_permission(db, permission: ImagePermissionModel, apply_changes: bool,
                      resource_curies_by_id):
    """Delete one permission and its links. Returns the affected resource
    curies, or None (a problem) when a link was created by someone other than
    the old loader."""
    links = (db.query(ResourceImagePermissionModel)
             .filter_by(image_permission_id=permission.image_permission_id)
             .order_by(ResourceImagePermissionModel.resource_image_permission_id).all())
    foreign = [link for link in links
               if link.created_by is not None and link.created_by != OLD_LOADER_USER]
    if foreign:
        logger.warning(
            f"NOT deleting '{permission.name}': link(s) "
            f"{[link.resource_image_permission_id for link in foreign]} were created by "
            f"{sorted({link.created_by for link in foreign})} — curators must decide")
        return None
    curies = []
    logger.info(f"delete image_permission {permission.image_permission_id} "
                f"'{permission.name}' ({len(links)} link(s))")
    for link in links:
        curie = resource_curies_by_id.get(link.resource_id, f"resource_id={link.resource_id}")
        curies.append(curie)
        note = ""
        if link.updated_by and link.updated_by not in EXPECTED_EDITORS:
            note = f" (last edited by {link.updated_by})"
        logger.info(f"  delete link {link.resource_image_permission_id}: {curie} "
                    f"({link.start_year or ''}-{link.end_year or ''}){note}")
        if apply_changes:
            db.delete(link)
    if apply_changes:
        db.flush()
        db.delete(permission)
    return curies


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true",
                        help="commit the deletions (default is dry run)")
    args = parser.parse_args()

    db = create_postgres_session(False)
    set_global_user_id(db, SCRIPT_USER)
    problems = 0
    deleted_permissions = 0
    deleted_links = 0
    try:
        old_rows = (db.query(ImagePermissionModel)
                    .filter_by(created_by=OLD_LOADER_USER)
                    .order_by(ImagePermissionModel.name).all())
        for name in EXTRA_OLD_SHEET_PERMISSION_NAMES:
            extra = db.query(ImagePermissionModel).filter_by(name=name).one_or_none()
            if extra is None:
                logger.info(f"no row named '{name}' (already removed)")
            elif extra.created_by is not None and extra.created_by != OLD_LOADER_USER:
                problems += 1
                logger.warning(f"NOT deleting '{name}': created by {extra.created_by}, "
                               f"not the old loader — curators must decide")
            else:
                old_rows.append(extra)

        link_rows = (db.query(ResourceImagePermissionModel.resource_id, ResourceModel.curie)
                     .join(ResourceModel,
                           ResourceModel.resource_id == ResourceImagePermissionModel.resource_id)
                     .distinct().all())
        resource_curies_by_id = {row.resource_id: row.curie for row in link_rows}

        affected_curies = set()
        for permission in old_rows:
            curies = delete_permission(db, permission, args.apply, resource_curies_by_id)
            if curies is None:
                problems += 1
            else:
                deleted_permissions += 1
                deleted_links += len(curies)
                affected_curies.update(curies)

        logger.info(f"summary: {deleted_permissions} permission(s), {deleted_links} link(s) "
                    f"across {len(affected_curies)} resource(s); {problems} problem(s)")
        if args.apply and problems == 0:
            db.commit()
            logger.info("[APPLY] committed")
        elif args.apply:
            db.rollback()
            logger.error(f"[APPLY] rolled back: {problems} problem(s) above")
        else:
            db.rollback()
            logger.info("[DRY RUN] nothing committed")
    finally:
        db.close()


if __name__ == "__main__":
    main()
