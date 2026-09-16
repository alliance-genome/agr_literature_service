#!/usr/bin/env python3
"""
Delete the resource_image_permission links superseded by the image working
group's copyright sheet (SCRUM-6416), so load_alliance_copyright_permissions.py
can take over those (resource, year-range) slots.

The alliance loader refuses to load onto a slot that already carries a foreign
grant; the 2026-09-16 dev dry run reported exactly four such conflicts, all
grants from the older journal_permission.tsv load that the working-group sheet
supersedes:

- Genetics and G3: "Genetic Society of America (GSA), Oxford University
  Press? - Blanket Permission" -> journal-level full-permission grants.
- Biochem J: "Portland Press LTD - Permission Granted (subset)" -> Portland
  Press full permission. The same old permission's Biol Cell link is NOT
  touched (Biology of the Cell is not a working-group journal).
- J Neurosci (through 2009): "Society for Neuroscience - Open Access" -> the
  pre-2010 copyright-statement grant. The same permission's eNeuro link is NOT
  touched (eNeuro is skip=yes in the seed, pending the working group).

Links are matched by (resource curie, permission name), which is stable across
dev/stage/prod (dev is a prod dump); a permission left with zero links is also
deleted. Deletes go through the ORM session so the version/audit tables record
them. Dry-run by default:

    python delete_superseded_image_permission_links.py            # dry run
    python delete_superseded_image_permission_links.py --apply
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

# (resource curie, superseded permission name, replacing alliance grant)
SUPERSEDED_LINKS = [
    ("AGRKB:102000000003721",
     "Genetic Society of America (GSA), Oxford University Press? - Blanket Permission",
     "Genetics: full permission (journal grant, not OUP)"),
    ("AGRKB:102000000024800",
     "Genetic Society of America (GSA), Oxford University Press? - Blanket Permission",
     "G3: full permission (journal grant, not OUP)"),
    ("AGRKB:102000000000974",
     "Portland Press LTD - Permission Granted (subset)",
     "Portland Press: full permission"),
    ("AGRKB:102000000004868",
     "Society for Neuroscience - Open Access",
     "Society for Neuroscience: copyright statement (pre-2010)"),
]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true",
                        help="commit the deletions (default is dry run)")
    args = parser.parse_args()

    db = create_postgres_session(False)
    set_global_user_id(db, SCRIPT_USER)
    touched_permission_ids = set()
    problems = 0
    try:
        for curie, permission_name, replacement in SUPERSEDED_LINKS:
            links = (
                db.query(ResourceImagePermissionModel)
                .join(ImagePermissionModel)
                .join(ResourceModel,
                      ResourceModel.resource_id == ResourceImagePermissionModel.resource_id)
                .filter(ResourceModel.curie == curie,
                        ImagePermissionModel.name == permission_name)
                .all()
            )
            if len(links) != 1:
                problems += 1
                logger.warning(
                    f"expected exactly 1 link for {curie} x '{permission_name}', "
                    f"found {len(links)}; NOT deleting")
                continue
            link = links[0]
            touched_permission_ids.add(link.image_permission_id)
            logger.info(
                f"delete link {link.resource_image_permission_id}: {curie} -> "
                f"'{permission_name}' (superseded by '{replacement}')")
            if args.apply:
                db.delete(link)

        if args.apply:
            db.flush()
        for permission_id in sorted(touched_permission_ids):
            remaining = (
                db.query(ResourceImagePermissionModel)
                .filter_by(image_permission_id=permission_id)
                .count()
            )
            if remaining == 0:
                permission = db.query(ImagePermissionModel).get(permission_id)
                logger.info(f"delete orphaned image_permission {permission_id} "
                            f"'{permission.name if permission else '?'}'")
                if args.apply:
                    db.delete(permission)
            else:
                logger.info(f"keep image_permission {permission_id}: "
                            f"{remaining} other link(s) remain")

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
