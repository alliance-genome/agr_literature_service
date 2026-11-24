"""SCRUM-6729: hand the WB placeholder automation agents to their persons where ABC already has them.

The Caltech pipelines record their curators by bare WormBase person id (WBPerson1843). Before SCRUM-6729 every
such id became an automation agent of that name, even when ABC had the person (a WB:WBPerson1843 person cross
reference). From SCRUM-6729 on, the resolver gives a WB person id that matches a person to that person, and claims
the placeholder for them the first time it is used. This script does the same at once for every placeholder whose
person is already in ABC:

  - the person has no agent: the placeholder becomes their agent;
  - the person already has one: the placeholder becomes a second agent of the same person.

Either way the placeholder gets the person's id and loses its name, so every record it created or updated shows
the person's curie. Only agent rows change, never the records. Placeholders whose person is not in ABC are left
as they are; the WB person import (SCRUM-6458) claims them later the same way.

Run after the SCRUM-6456 run on the same database: that script finds the curation status form rows by their
WBPerson creator, and a claimed placeholder no longer looks like one. The script refuses to run while such rows
are left to move.

Usage (idempotent; dev, then prod after UAT):
    python3 SCRUM-6729_claim_wb_person_placeholders.py            # dry run: list what would be claimed
    python3 SCRUM-6729_claim_wb_person_placeholders.py --apply    # claim them
"""
import argparse
import logging
from os import environ
from typing import Dict, List

from sqlalchemy import text
from sqlalchemy.orm import Session

from agr_literature_service.api.crud.user_utils import claim_wb_placeholder, person_for_identifier
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_SQL_PLACEHOLDERS = text(r"""
SELECT automation_username
FROM agent
WHERE automation_username ~* '^WBPerson[0-9]+$'
ORDER BY id
""")

# SCRUM-6456 form rows still on WB's ABC source, not edited since the load
_SQL_FORM_ROWS_LEFT = text(r"""
SELECT count(*)
FROM curation_status c
JOIN mod m ON m.id = c.mod_id AND m.abbreviation = 'WB'
JOIN agent a ON a.id = c.created_by_id AND a.automation_username LIKE 'WBPerson%'
JOIN curation_status_source_association csa ON csa.curation_status_id = c.id
JOIN tag_source ts ON ts.id = csa.tag_source_id
    AND ts.source_method = 'abc_literature_system' AND ts.secondary_data_provider_id = m.id
WHERE (c.curation_status = 'ATP:0000299'
       OR (c.curation_status = 'ATP:0000239' AND c.topic IN ('ATP:0000061', 'ATP:0000062')))
  AND c.updated_by_id = c.created_by_id
  AND csa.updated_by_id = c.created_by_id
""")

_SQL_AGENT_COUNT = text("SELECT count(*) FROM agent")


def form_rows_left(db: Session) -> int:
    return db.execute(_SQL_FORM_ROWS_LEFT).scalar() or 0


def claim_placeholders(db: Session, apply: bool = False) -> Dict[str, List[str]]:
    """Claim every placeholder whose person is in ABC. Returns {"claimed": [...], "not in ABC": [...]}, each
    entry 'WBPerson1843 -> AGRKB:...' or the bare id."""
    placeholders = list(db.execute(_SQL_PLACEHOLDERS).scalars())
    result: Dict[str, List[str]] = {"claimed": [], "not in ABC": []}
    agents_before = db.execute(_SQL_AGENT_COUNT).scalar()
    for placeholder in placeholders:
        person = claim_wb_placeholder(db, placeholder) if apply else person_for_identifier(db, placeholder)
        if person:
            result["claimed"].append(f"{placeholder} -> {person}")
        else:
            result["not in ABC"].append(placeholder)
    if apply:
        # claiming only re-points existing agents
        assert db.execute(_SQL_AGENT_COUNT).scalar() == agents_before, "claiming created agents"
        db.commit()
    logger.info(f"{len(placeholders)} WB placeholder agents: {len(result['claimed'])} "
                f"{'claimed' if apply else 'to claim'}, {len(result['not in ABC'])} not in ABC yet")
    for entry in result["claimed"]:
        logger.info(f"  {entry}")
    return result


def main():
    parser = argparse.ArgumentParser(description="SCRUM-6729: hand WB placeholder agents to their persons")
    parser.add_argument("--apply", action="store_true", help="commit the change; without it nothing is written")
    args = parser.parse_args()

    db = create_postgres_session(False)
    logger.info(f"database {environ.get('PSQL_HOST', 'localhost')}/{environ.get('PSQL_DATABASE', '')}")
    left = form_rows_left(db)
    if left:
        logger.error(f"{left} WB curation status form rows are still on the ABC source: run "
                     "SCRUM-6456_attribute_wb_curation_status_form.py --apply on this database first")
        raise SystemExit(1)
    claim_placeholders(db, apply=args.apply)
    if not args.apply:
        db.rollback()
        logger.info("dry run: nothing written; pass --apply")


if __name__ == "__main__":
    main()
