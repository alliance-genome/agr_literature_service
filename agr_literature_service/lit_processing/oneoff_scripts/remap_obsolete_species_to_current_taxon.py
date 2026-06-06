#!/usr/bin/env python3
"""
Remap obsolete/merged NCBITaxon species on topic_entity_tag rows to their current
primary NCBITaxon ID.

Some species curies used on topic_entity_tag rows are obsolete in NCBI Taxonomy
(merged into, or treated as synonyms of, a different current taxon). The A-Team
NCBITaxon table has no node for those exact IDs, so they cannot be imported by
load_missing_species_to_ateam.py and are flagged by
report_obsolete_disappeared_species_ids.py / report_obsolete_entities.py.

The fix is not to load them (there is nothing to load) but to update the tags to
the current primary NCBITaxon ID. This script:
  1. Finds the obsolete species curies in topic_entity_tag (the same detection the
     loader/report use: distinct species not present in the A-Team NCBITaxon table),
     or a caller-supplied subset via --curies.
  2. Resolves each obsolete taxid to its current primary taxid via NCBI E-utilities
     efetch (db=taxonomy) -- a merged/secondary ID resolves to a different primary.
  3. Optionally imports the resolved current species into A-Team (--ensure-target).
  4. Updates the referencing topic_entity_tag rows: the species column, and the
     entity column where topic = 'ATP:0000123' (species), from old curie to new.

Dry-run by default; pass --apply to commit changes.

Usage:
    # Dry run -- report the obsolete->current mapping and affected rows:
    python remap_obsolete_species_to_current_taxon.py

    # Limit to specific curies:
    python remap_obsolete_species_to_current_taxon.py --curies NCBITaxon:2778,NCBITaxon:151524

    # Apply, importing the resolved target species into A-Team first:
    python remap_obsolete_species_to_current_taxon.py --apply --ensure-target
"""
import argparse
import logging
import time
from os import environ
from typing import Dict, List, Optional
from xml.etree import ElementTree as ET

import requests
from sqlalchemy.orm import Session

from agr_curation_api import AGRCurationAPIClient, AGRAPIError  # type: ignore

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.models import TopicEntityTagModel
from agr_literature_service.lit_processing.data_check.load_missing_species_to_ateam import (
    get_distinct_species_curies,
    get_missing_species_curies,
    species_atp,
)

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

EFETCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
NCBITAXON_PREFIX = "NCBITaxon:"


def _taxid_num(curie: str) -> str:
    """Strip the NCBITaxon: prefix, returning the bare numeric taxid."""
    return curie.split(":", 1)[1] if ":" in curie else curie


def resolve_current_taxid(old_curie: str, call_delay: float = 0.34) -> Optional[str]:
    """Resolve a (possibly merged) NCBITaxon curie to its current primary curie.

    efetch on a merged/secondary taxid returns the current primary record; its
    <TaxId> is the current id and the queried id appears under <AkaTaxIds>.
    Returns the current NCBITaxon: curie, or None if it cannot be resolved.
    """
    params = {"db": "taxonomy", "id": _taxid_num(old_curie), "retmode": "xml"}
    api_key = environ.get("NCBI_API_KEY")
    if api_key:
        params["api_key"] = api_key
    try:
        resp = requests.get(EFETCH_URL, params=params, timeout=30)
        resp.raise_for_status()
        root = ET.fromstring(resp.text)
    except (requests.RequestException, ET.ParseError) as e:
        logger.warning(f"  NCBI lookup failed for {old_curie}: {e}")
        return None
    finally:
        if call_delay:
            time.sleep(call_delay)

    taxon = root.find("Taxon")
    if taxon is None:
        return None
    taxid_el = taxon.find("TaxId")
    if taxid_el is None or not taxid_el.text:
        return None
    return f"{NCBITAXON_PREFIX}{taxid_el.text.strip()}"


def build_remap(obsolete_curies: List[str]) -> Dict[str, str]:
    """Return {old_curie: new_curie} only for curies that resolve to a DIFFERENT
    current primary taxid. Curies that resolve to themselves (still primary in NCBI,
    so not actually merged) or cannot be resolved are skipped with a log line.
    """
    remap: Dict[str, str] = {}
    for old in obsolete_curies:
        new = resolve_current_taxid(old)
        if new is None:
            logger.info(f"  {old}: could not resolve via NCBI -- skipping (verify manually)")
            continue
        if new == old:
            logger.info(f"  {old}: still primary in NCBI (not merged) -- not a remap; "
                        f"the loader should import it")
            continue
        logger.info(f"  {old} -> {new}")
        remap[old] = new
    return remap


def ensure_targets_loaded(remap: Dict[str, str], client: AGRCurationAPIClient) -> None:
    """Import each resolved current species into A-Team so the remapped tags resolve."""
    for new in sorted(set(remap.values())):
        try:
            term = client.get_or_create_species(new)
            logger.info(f"  ensured target {new} -> {term.name or ''}")
        except AGRAPIError as e:
            logger.warning(f"  failed to ensure target {new} in A-Team: {e}")


def apply_remap(db: Session, remap: Dict[str, str], dry_run: bool) -> int:
    """Update topic_entity_tag.species and .entity (species tags) from old to new.

    Returns the number of column updates performed (or that would be performed).
    """
    total = 0
    for old, new in remap.items():
        species_tags = (
            db.query(TopicEntityTagModel)
            .filter(TopicEntityTagModel.species == old)
            .all()
        )
        entity_tags = (
            db.query(TopicEntityTagModel)
            .filter(TopicEntityTagModel.entity == old,
                    TopicEntityTagModel.topic == species_atp)
            .all()
        )
        logger.info(f"{old} -> {new}: {len(species_tags)} species-column row(s), "
                    f"{len(entity_tags)} entity-column row(s)")

        for tag in species_tags:
            logger.info(f"    tag {tag.topic_entity_tag_id} (ref {tag.reference_id}) "
                        f"species: {old} -> {new}")
            if not dry_run:
                tag.species = new
            total += 1

        for tag in entity_tags:
            logger.info(f"    tag {tag.topic_entity_tag_id} (ref {tag.reference_id}) "
                        f"entity: {old} -> {new}")
            if not dry_run:
                tag.entity = new
            total += 1

    if not dry_run:
        db.commit()
        logger.info(f"Committed {total} column update(s).")
    else:
        logger.info(f"DRY RUN: would perform {total} column update(s). Re-run with --apply.")
    return total


def remap_obsolete_species(curies: Optional[List[str]] = None, dry_run: bool = True,
                           ensure_target: bool = False) -> None:

    db_session = create_postgres_session(False)
    try:
        if curies:
            obsolete = sorted({c.strip() for c in curies if c.strip()})
            logger.info(f"Using {len(obsolete)} caller-supplied curie(s).")
        else:
            all_curies = get_distinct_species_curies(db_session)
            obsolete = get_missing_species_curies(all_curies)
            logger.info(f"{len(obsolete)} species curie(s) in topic_entity_tag are "
                        f"missing from the A-Team NCBITaxon table.")

        if not obsolete:
            logger.info("No obsolete species to remap.")
            return

        logger.info("Resolving current primary taxids via NCBI...")
        remap = build_remap(obsolete)
        if not remap:
            logger.info("No obsolete->current remappings to apply.")
            return

        if ensure_target:
            logger.info("Ensuring resolved target species exist in A-Team...")
            ensure_targets_loaded(remap, AGRCurationAPIClient())

        apply_remap(db_session, remap, dry_run=dry_run)
    except Exception:
        db_session.rollback()
        raise
    finally:
        db_session.close()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Remap obsolete/merged NCBITaxon species on topic_entity_tag rows "
                    "to their current primary NCBITaxon ID."
    )
    parser.add_argument("--apply", action="store_true",
                        help="Commit changes. Without this flag the script is a dry run.")
    parser.add_argument("--curies", type=str, default=None,
                        help="Comma-separated NCBITaxon curies to remap "
                             "(default: auto-detect obsolete species from topic_entity_tag).")
    parser.add_argument("--ensure-target", action="store_true",
                        help="Import each resolved current species into A-Team before remapping.")
    args = parser.parse_args()

    curie_list = args.curies.split(",") if args.curies else None
    remap_obsolete_species(curies=curie_list, dry_run=not args.apply,
                           ensure_target=args.ensure_target)
