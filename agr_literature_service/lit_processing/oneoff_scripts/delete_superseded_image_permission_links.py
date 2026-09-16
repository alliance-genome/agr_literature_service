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

A second wave covers old grants that overlap the alliance grants on DIFFERENT
year ranges (so the loader's exact-slot guard correctly let both exist): the
split-range Rockefeller grants on J Cell Biol / J Exp Med / J Gen Physiol and
the SfN 2011- CC grant on J Neurosci. Folia Biologica is deliberately NOT
cleaned: its old grant names Charles Univ Prague (the publisher of Folia
Biologica (Praha), a different journal), so curators must first untangle which
journal the permissions actually belong to.

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

# (resource curie, superseded permission name, replacing alliance grant,
#  max links expected). All links for the (curie, name) pair are deleted, up
# to the expected count: the second-wave Rockefeller grants were split into
# two year ranges each (through 2007 / from 2009) by the old loader's
# before/after parsing, and both halves are superseded together. Finding MORE
# links than expected aborts that pair (the name matched something new since
# this script was written); finding zero just means a previous run (or the
# first-wave cleanup on dev) already removed it.
SUPERSEDED_LINKS = [
    # first wave: exact-slot conflicts from the 2026-09-16 dev dry run
    ("AGRKB:102000000003721",
     "Genetic Society of America (GSA), Oxford University Press? - Blanket Permission",
     "Genetics: full permission (journal grant, not OUP)", 1),
    ("AGRKB:102000000024800",
     "Genetic Society of America (GSA), Oxford University Press? - Blanket Permission",
     "G3: full permission (journal grant, not OUP)", 1),
    ("AGRKB:102000000000974",
     "Portland Press LTD - Permission Granted (subset)",
     "Portland Press: full permission", 1),
    ("AGRKB:102000000004868",
     "Society for Neuroscience - Open Access",
     "Society for Neuroscience: copyright statement (pre-2010)", 1),
    # second wave: overlapping-range grants on the working-group journals.
    # The Rockefeller links carry the note 'Reach out to them just to make
    # sure we could reproduce images for ALL MODS past and future' - the
    # working-group sheet IS the answer to that reach-out (full permission,
    # all years). The SfN 2011- grant is replaced by the working group's
    # ranged 2010-2014 / 2015-2025 / 2026- structure.
    ("AGRKB:102000000004422",
     "The Rockefeller University Press - Contract",
     "Rockefeller University Press: full permission", 2),
    ("AGRKB:102000000004560",
     "The Rockefeller University Press - Open Access",
     "Rockefeller University Press: full permission", 2),
    ("AGRKB:102000000004581",
     "The Rockefeller University Press - Open Access",
     "Rockefeller University Press: full permission", 2),
    ("AGRKB:102000000004868",
     "Society for Neuroscience - CC BY 4.0 and CC-BY-NC-SA | Open Access",
     "Society for Neuroscience: CC-BY-NC-SA 3.0 (2010-2014) / CC-BY 4.0 (2015-2025) / 2026- grants", 1),
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
        for curie, permission_name, replacement, max_links in SUPERSEDED_LINKS:
            links = (
                db.query(ResourceImagePermissionModel)
                .join(ImagePermissionModel)
                .join(ResourceModel,
                      ResourceModel.resource_id == ResourceImagePermissionModel.resource_id)
                .filter(ResourceModel.curie == curie,
                        ImagePermissionModel.name == permission_name)
                .all()
            )
            if len(links) == 0:
                logger.info(f"no links for {curie} x '{permission_name}' (already removed)")
                continue
            if len(links) > max_links:
                problems += 1
                logger.warning(
                    f"expected at most {max_links} link(s) for {curie} x '{permission_name}', "
                    f"found {len(links)}; NOT deleting")
                continue
            for link in links:
                touched_permission_ids.add(link.image_permission_id)
                logger.info(
                    f"delete link {link.resource_image_permission_id}: {curie} -> "
                    f"'{permission_name}' "
                    f"({link.start_year or ''}-{link.end_year or ''}; superseded by '{replacement}')")
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
