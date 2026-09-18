#!/usr/bin/env python3
"""
Fix curator-reported problems in the image_permission vocabulary:

1. Spelling of the permission names. Two rows misspell Wiley as "Wylie", and
   the Wiley-Blackwell rows are split between "Wiley Blackwell" and
   "Wiley-Blackwell". Per the curators the NLM catalog is the source of truth
   for publisher names, and it hyphenates: Wiley-Blackwell. Wiley-Blackwell is
   a subsidiary of Wiley with its own permissions and attribution text, so the
   two publishers are deliberately NOT merged into each other -- only the
   spelling variants of each are normalised.

   Renaming can collide with an existing row ("Wiley Blackwell - Contract" and
   "Wiley-Blackwell - Contract" both exist, and image_permission.name is
   unique), so a rename whose target name already exists becomes a merge: the
   source row's resource links are repointed to the target and the emptied
   source row is deleted. A merge only proceeds when the two rows carry the
   same permission payload (text, urls, can_display_images) -- differing rows
   are reported and left alone for curators.

2. MOD mentions in the Alliance attribution text (permission_text). The field
   is Alliance-level, but 15 rows open with "WormBase thanks ..." / "WormBase
   wishes to thank ...". The MOD name is replaced with "The Alliance", per the
   curators; the rest of each text is preserved. ("WormBook" in the WormBook
   permission is the *grantor*, not a MOD mention, and is not touched.)

3. The "<Articlel_URL>" placeholder typo (2 rows), which would never be
   substituted by consumers looking for "<Article_URL>".

Rows are matched by name/text, never by id: dev is a prod dump, so ids drift
across dev/stage/prod while names are stable. All writes go through the ORM
session so the version/audit tables record them. Dry-run by default:

    python fix_image_permission_spellings_and_attribution.py          # dry run
    python fix_image_permission_spellings_and_attribution.py --apply
"""

import argparse
import logging
from os import path

from agr_literature_service.api.models import (
    ImagePermissionModel,
    ResourceImagePermissionModel,
)
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logging.basicConfig(format="%(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

SCRIPT_USER = path.basename(__file__).replace(".py", "")

# old name -> corrected name. Where the corrected name already exists the fix
# is a merge (see module docstring); otherwise a plain rename.
NAME_FIXES = {
    "Wylie - CC BY 4.0":
        "Wiley - CC BY 4.0",
    "Wylie - CC BY and CC BY-NC and CC BY-NC-ND | Hybrid":
        "Wiley - CC BY and CC BY-NC and CC BY-NC-ND | Hybrid",
    "Wiley Blackwell - Contract":
        "Wiley-Blackwell - Contract",
    "Wiley Blackwell - Publisher Permission":
        "Wiley-Blackwell - Publisher Permission",
}

# Substring replacements applied to permission_text. Plain substrings, not
# regexes: the texts are short controlled prose, and every current occurrence
# is a literal match.
TEXT_FIXES = [
    ("WormBase", "The Alliance"),
    ("<Articlel_URL>", "<Article_URL>"),
]


def _payloads_match(source: ImagePermissionModel, target: ImagePermissionModel) -> bool:
    """Whether two rows are interchangeable, so merging loses no information."""
    return ((source.permission_text or "").strip() == (target.permission_text or "").strip()
            and (source.permission_url or "") == (target.permission_url or "")
            and (source.permission_doc_url or "") == (target.permission_doc_url or "")
            and source.can_display_images == target.can_display_images)


def _link_slot(link: ResourceImagePermissionModel):
    """The unique slot a link occupies (uq_resource_image_permission_range)."""
    return (link.resource_id, link.start_year, link.end_year)


def merge_permission(db, source: ImagePermissionModel, target: ImagePermissionModel,
                     apply_changes: bool) -> bool:
    """Repoint source's resource links onto target and delete source.

    Returns False (a problem) when the payloads differ. A source link whose
    (resource, year-range) slot already exists on the target is deleted rather
    than repointed -- the grant is already recorded there.
    """
    if not _payloads_match(source, target):
        logger.warning(
            f"NOT merging '{source.name}' into '{target.name}': the rows carry different "
            f"permission payloads; curators need to reconcile them first")
        return False
    taken_slots = {
        _link_slot(link)
        for link in db.query(ResourceImagePermissionModel)
        .filter_by(image_permission_id=target.image_permission_id).all()
    }
    links = (db.query(ResourceImagePermissionModel)
             .filter_by(image_permission_id=source.image_permission_id).all())
    for link in links:
        if _link_slot(link) in taken_slots:
            logger.info(
                f"  delete link {link.resource_image_permission_id} (resource "
                f"{link.resource_id}): same slot already granted on '{target.name}'")
            if apply_changes:
                db.delete(link)
        else:
            logger.info(
                f"  repoint link {link.resource_image_permission_id} (resource "
                f"{link.resource_id}) -> '{target.name}'")
            if apply_changes:
                link.image_permission_id = target.image_permission_id
    logger.info(f"  delete emptied image_permission {source.image_permission_id} "
                f"'{source.name}'")
    if apply_changes:
        db.flush()
        db.delete(source)
    return True


def fix_names(db, apply_changes: bool) -> int:
    problems = 0
    for old_name, new_name in NAME_FIXES.items():
        source = db.query(ImagePermissionModel).filter_by(name=old_name).one_or_none()
        if source is None:
            logger.info(f"no row named '{old_name}' (already fixed)")
            continue
        target = db.query(ImagePermissionModel).filter_by(name=new_name).one_or_none()
        if target is None:
            logger.info(f"rename '{old_name}' -> '{new_name}'")
            if apply_changes:
                source.name = new_name
        else:
            logger.info(f"merge '{old_name}' into existing '{new_name}'")
            if not merge_permission(db, source, target, apply_changes):
                problems += 1
    return problems


def fix_texts(db, apply_changes: bool) -> None:
    for old_text, new_text in TEXT_FIXES:
        rows = (db.query(ImagePermissionModel)
                .filter(ImagePermissionModel.permission_text.contains(old_text))
                .order_by(ImagePermissionModel.image_permission_id).all())
        if not rows:
            logger.info(f"no permission_text contains '{old_text}' (already fixed)")
            continue
        for row in rows:
            logger.info(f"replace '{old_text}' -> '{new_text}' in permission_text of "
                        f"{row.image_permission_id} '{row.name}'")
            if apply_changes:
                row.permission_text = row.permission_text.replace(old_text, new_text)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true",
                        help="commit the fixes (default is dry run)")
    args = parser.parse_args()

    db = create_postgres_session(False)
    set_global_user_id(db, SCRIPT_USER)
    try:
        problems = fix_names(db, args.apply)
        fix_texts(db, args.apply)
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
