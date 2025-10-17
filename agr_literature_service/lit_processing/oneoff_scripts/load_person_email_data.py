import logging
from os import path
from typing import Iterable, List, Optional
from sqlalchemy.orm import Session

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.models import (
    UserModel,
    PersonModel,
    EmailModel,
    PersonCrossReferenceModel,
)
from agr_literature_service.api.user import set_global_user_id, get_current_user_pk

logging.basicConfig(format="%(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DATAFILE = "data/okta_person_mapping.tsv"


def normalize_roles(raw: str) -> List[str]:
    """
    Convert a whitespace- or comma-separated role string into a clean list.
    Removes empties and deduplicates while preserving a stable order.
    """
    if not raw:
        return []
    # split on any whitespace; if the data uses commas, add replace(",", " ")
    tokens = [t.strip() for t in raw.replace(",", " ").split() if t.strip()]
    # dedupe but keep order
    seen = set()
    out: List[str] = []
    for t in tokens:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def as_single_spaced(s: str) -> str:
    return " ".join(s.split())


def load_data() -> None:
    db = create_postgres_session(False)

    # mark this run as executed by the script "user" (automation)
    scriptNm = path.basename(__file__).replace(".py", "")
    set_global_user_id(db, scriptNm)
    logger.info("transaction user pk:", get_current_user_pk(db))

    created_count = 0
    updated_count = 0
    skipped_count = 0
    error_count = 0

    with open(DATAFILE, "r", encoding="utf-8") as f:
        for ln, line in enumerate(f, start=1):
            if not line.strip() or line.startswith("#"):
                continue

            items = line.rstrip("\n").split("\t")

            # We reference indices 0,1,2,6,7,10,12 below → need len >= 13
            if len(items) < 13:
                skipped_count += 1
                logger.info(f"line {ln}: skipped (expected >=13 columns, got {len(items)})")
                continue

            # only process 'primary' rows
            if items[10].strip() != "primary":
                continue

            okta = items[0].strip()
            email = items[1].strip()
            display_name = as_single_spaced(items[2].strip())
            mod_id = items[6].strip()
            orcid = items[7].strip()
            mod_roles = normalize_roles(items[12].strip())

            if not okta:
                skipped_count += 1
                logger.info(f"line {ln}: skipped (missing okta id)")
                continue

            try:
                person_id = insert_person(db, display_name, okta, mod_roles)
                insert_email(db, person_id, email)

                # ORCID xref (only if present)
                if orcid:
                    curie_prefix = "ORCID"
                    curie = f"{curie_prefix}:{orcid}"
                    insert_person_xref(db, person_id, curie, curie_prefix)

                # MOD xref (only when prefix can be inferred)
                if mod_id:
                    curie_prefix: Optional[str] = None
                    if mod_id.startswith("ZDB"):
                        curie_prefix = "ZFIN"
                    elif mod_id.startswith("XB"):
                        curie_prefix = "XenBase"
                    elif mod_id.startswith("WB"):
                        curie_prefix = "WB"

                    if curie_prefix:  # only insert when we know the prefix
                        curie = f"{curie_prefix}:{mod_id}"
                        insert_person_xref(db, person_id, curie, curie_prefix)

                # Link users.id (Okta) to this person (and clear automation_username)
                created, updated = update_users_link_person(db, okta, person_id)
                created_count += int(created)
                updated_count += int(updated)

                logger.info(
                    f"added/updated person {display_name}: okta={okta} email={email} "
                    f"orcid={orcid or '-'} mod_id={mod_id or '-'} roles={mod_roles}"
                )

            except Exception as e:
                db.rollback()
                error_count += 1
                logger.info(
                    f"ERROR line {ln} for okta={okta} display_name={display_name!r}: {e}"
                )

    # final commit to persist all successful rows
    db.commit()
    # db.rollback()
    logger.info(
        f"Done. created_users={created_count} updated_users={updated_count} "
        f"skipped={skipped_count} errors={error_count}"
    )


def update_users_link_person(db: Session, okta_id: str, person_id: int) -> tuple[bool, bool]:
    """
    Make sure a users row exists for this okta id and points to person_id.
    Enforces the 'exactly one of (person_id, automation_username)' invariant:
      - If row does not exist → create with person_id set, automation_username=NULL
      - If row exists as automation → set person_id and NULL automation_username
      - If row exists and already linked → no-op
    Returns (created: bool, updated: bool)
    """
    u = db.query(UserModel).filter_by(id=okta_id).one_or_none()
    if u is None:
        # create human user (person-linked)
        x = UserModel(id=okta_id, person_id=person_id, automation_username=None)
        db.add(x)
        # no need to flush here; will be committed at the batch end
        return True, False

    updated = False

    # If it was an automation user, flip to human-linked and clear automation_username
    if u.person_id != person_id or u.automation_username is not None:
        u.person_id = person_id
        u.automation_username = None  # IMPORTANT to satisfy the CHECK constraint
        updated = True

    db.add(u)
    return False, updated


def insert_person_xref(db: Session, person_id: int, curie: str, curie_prefix: str) -> None:
    # PersonCrossReferenceModel.curie_prefix is NOT NULL → enforce here
    x = PersonCrossReferenceModel(
        person_id=person_id,
        curie=curie,
        curie_prefix=curie_prefix,
    )
    db.add(x)


def insert_email(db: Session, person_id: int, email: str) -> None:
    # Normalize email (lowercase + strip)
    em = email.strip().lower()
    if not em:
        return
    x = EmailModel(
        person_id=person_id,
        email_address=em,
    )
    db.add(x)


def insert_person(db: Session, display_name: str, okta: str, mod_roles: Iterable[str]) -> int:
    """
    Insert a Person and return its person_id.
    Uses flush to obtain PK without committing early.
    """
    x = PersonModel(
        display_name=as_single_spaced(display_name) or okta,
        okta_id=okta,
        mod_roles=list(mod_roles) if mod_roles else None,
    )
    db.add(x)
    db.flush()
    db.refresh(x)
    return x.person_id


if __name__ == "__main__":
    load_data()
