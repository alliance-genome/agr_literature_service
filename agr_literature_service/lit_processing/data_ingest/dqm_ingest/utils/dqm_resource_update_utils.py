"""
Functions to update and create resources.

sanitized json entry mentioned in the docs is the original dqm entry that has been
modified only wrt its primary_id and a cross reference added of the primary id if
it was not already in the cross references in the file.

NOTE:The script part of this has been removed as it is no longer used that way.
The functions now processes dicts that are modified from the original dqm format.
So no files are read anymore.
"""
import logging
import warnings
from typing import Any, Dict, List, Tuple, Optional
from sqlalchemy.orm import Session

from dotenv import load_dotenv
from fastapi.encoders import jsonable_encoder

from agr_literature_service.api.models import ResourceModel
from sqlalchemy.orm.exc import NoResultFound
from agr_literature_service.lit_processing.utils.generic_utils import split_identifier
from agr_literature_service.lit_processing.data_ingest.dqm_ingest.utils.dqm_processing_utils import \
    compare_authors_or_editors
from agr_literature_service.lit_processing.utils.tmp_files_utils import init_tmp_dir
from agr_literature_service.lit_processing.data_ingest.post_resource_to_db import \
    process_resource_entry
from agr_literature_service.lit_processing.utils.resource_reference_utils import (
    get_agr_for_xref,
    agr_has_xref_of_prefix,
    is_obsolete,
    add_xref,
    find_existing_resource
)

warnings.filterwarnings("ignore", category=UserWarning, module='bs4')

load_dotenv()
init_tmp_dir()

remap_keys: Dict = {}
simple_fields: List = []
list_fields: List = []
resources_to_update: Dict = dict()

# Flags for the end processing result
PROCESSED_NEW = 0
PROCESSED_UPDATED = 1
PROCESSED_FAILED = 2
PROCESSED_NO_CHANGE = 3

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

batch_size_for_commit = 250


def reset_resources_to_update() -> None:
    """
    Clear the in-memory tracking dict used to prevent multiple updates to the same AGR in one run.
    Kept for backward compatibility with callers/imports.
    """
    global resources_to_update
    resources_to_update = dict()


def _merge_case_insensitive(db_values: List, incoming_values: List) -> List[str]:
    """
    Add-only merge for lists of strings.
    Keep existing DB values, append any new values from incoming list (case-insensitive uniqueness).
    """
    merged: List[str] = []
    seen = set()

    # keep DB values first (preserve their existing text)
    for v in (db_values or []):
        if not isinstance(v, str):
            continue
        s = v.strip()
        if not s:
            continue
        k = s.lower()
        if k not in seen:
            seen.add(k)
            merged.append(s)

    # add any missing from incoming
    for v in (incoming_values or []):
        if not isinstance(v, str):
            continue
        s = v.strip()
        if not s:
            continue
        k = s.lower()
        if k not in seen:
            seen.add(k)
            merged.append(s)

    return merged


def has_nlm_id(db_entry: Dict[str, Any], dqm_entry: Dict[str, Any]) -> bool:
    """
    Robust NLM detection using ONLY the entry shapes we can see here.

    NOTE: DB relationships are often not serialized into db_entry, so this is not sufficient alone.
    In process_update_resource() we OR this with agr_has_xref_of_prefix(agr, "NLM") (xref cache).

    Detection priority:
      1) NLM cross-references if present (DB-side or DQM-side): "NLM:<id>"
      2) Explicit NLM field if present in DQM payload (e.g. nlm/nlmId/nlm_id)
      3) Last-resort heuristic: primaryId matches digits + trailing "R" (e.g. "2985117R")
         This is a heuristic and may cause false positives; xrefs remain preferred.
    """
    # ---- 1) Check DB cross-references if present in jsonable_encoder output ----
    db_xrefs = db_entry.get("cross_references") or db_entry.get("crossReferences") or []
    for xr in db_xrefs:
        if isinstance(xr, dict):
            # Some serialized forms may split prefix vs id
            if str(xr.get("curie_prefix", "")).upper() == "NLM":
                return True

            curie = xr.get("curie") or xr.get("id")
            if isinstance(curie, str) and curie.startswith("NLM:"):
                return True

        elif isinstance(xr, str) and xr.startswith("NLM:"):
            return True

    # ---- 2) Check incoming DQM crossReferences ----
    for xr in (dqm_entry.get("crossReferences") or []):
        if not isinstance(xr, dict):
            continue
        cid = xr.get("id") or xr.get("curie")
        if isinstance(cid, str) and cid.startswith("NLM:"):
            return True

    # ---- 3) Check explicit NLM field in DQM payload (if present) ----
    nlm = dqm_entry.get("nlm") or dqm_entry.get("nlmId") or dqm_entry.get("nlm_id")
    if isinstance(nlm, str) and nlm.strip():
        return True

    # ---- 4) Last-resort: heuristic on primaryId (e.g., "2985117R") ----
    pid = dqm_entry.get("primaryId")
    if pid is not None:
        s = str(pid).strip()

        # Skip CURIE-like primaryIds (e.g., "ZFIN:...", "MGI:...")
        if ":" not in s:
            # Heuristic: digits followed by a trailing R/r.
            # Conservative min-length reduces accidental matches.
            if len(s) >= 6 and s[:-1].isdigit() and s[-1] in ("R", "r"):
                return True

    return False


def _is_shared_resource(db_entry: Dict, dqm_entry: Dict) -> bool:
    """
    Determine whether this resource is shared across MODs.

    Heuristic:
      - If DB already has >=2 MOD prefixes (FB/ZFIN/...) among its xrefs, shared.
      - Or if incoming entry xrefs / primaryId indicate it belongs to multiple MOD namespaces.

    NOTE: db_entry often does NOT include cross_references relationship; this heuristic will still
    work best when DB xrefs are present. For stability we rely mainly on "last mod wins"
    + NLM-controlled skip + add-only lists.
    """
    mod_prefixes = {"FB", "ZFIN", "MGI", "RGD", "WB", "SGD", "XB", "XBXL", "XBXT", "XBXS"}

    seen_mods = set()

    # DB xrefs (if serialized)
    db_xrefs = db_entry.get("cross_references") or db_entry.get("crossReferences") or []
    for xr in db_xrefs:
        if isinstance(xr, dict):
            p = xr.get("curie_prefix")
            if isinstance(p, str) and p in mod_prefixes:
                seen_mods.add(p)
            cid = xr.get("id")
            if isinstance(cid, str) and ":" in cid:
                prefix, _, _ = split_identifier(cid, ignore_error=True)
                if prefix in mod_prefixes:
                    seen_mods.add(prefix)
        elif isinstance(xr, str) and ":" in xr:
            prefix, _, _ = split_identifier(xr, ignore_error=True)
            if prefix in mod_prefixes:
                seen_mods.add(prefix)

    # DQM crossReferences
    for xr in dqm_entry.get("crossReferences", []) or []:
        if not isinstance(xr, dict):
            continue
        cid = xr.get("id")
        if not isinstance(cid, str) or ":" not in cid:
            continue
        prefix, _, _ = split_identifier(cid, ignore_error=True)
        if prefix in mod_prefixes:
            seen_mods.add(prefix)

    # DQM primaryId
    pid = dqm_entry.get("primaryId")
    if isinstance(pid, str) and ":" in pid:
        prefix, _, _ = split_identifier(pid, ignore_error=True)
        if prefix in mod_prefixes:
            seen_mods.add(prefix)

    return len(seen_mods) >= 2


def process_single_resource(
    db_session: Session,
    resource_dict: Dict,
    mod: str = "",
    writer_mod: str = ""
) -> Tuple:
    """
    Sorts out if the entry is new or an update and calls the appropriate
    function to create or update to the database.

    Uses comprehensive duplicate detection to check:
    1. primaryId cross-reference
    2. All cross-references in the entry
    3. ISSN values (print_issn, online_issn)

    :param db_session: db connection
    :param resource_dict: sanitized dqm json entry
    :param mod: current MOD ('' for NLM phase)
    :param writer_mod: MOD allowed to update shared resources
    :return: Tuple of (stat, process_okay, message, field_changes, missing_prefix_xrefs,
             xref_conflicts, xref_additions, failure_kind)
             where field_changes is a list of dicts with 'agr', 'primary_id', 'field',
             'old_value', 'new_value'
    """
    primary_id = resource_dict.get('primaryId')
    logger.info("primary_id %s resource_dict %s", primary_id, resource_dict)

    # Default outputs
    field_changes: List[Dict] = []
    missing_prefix_xrefs: List[str] = []
    xref_conflicts: List[str] = []
    xref_additions: List[Dict] = []
    message = ""
    failure_kind = "none"
    process_okay = True
    stat = PROCESSED_NO_CHANGE

    # Basic validation
    if not primary_id:
        message = "[invalid] Missing primaryId"
        logger.warning(message)
        return PROCESSED_FAILED, False, message, [], [], [], [], "invalid"

    # Use comprehensive duplicate detection
    existing = find_existing_resource(resource_dict, allow_title_match=False)

    if existing:
        agr, resource_id, match_type = existing
        logger.info(f"Found existing resource {agr} via {match_type} match for {primary_id}")

        if agr in resources_to_update:
            # Same AGR matched multiple times within the same MOD input.
            # Common when ISSNs/xrefs collapse different MOD rows onto one AGR resource.
            # Treat as harmless duplicate input row (skip) rather than a hard failure.
            prev = resources_to_update[agr]
            message = (
                f"SKIP duplicate input match for agr {agr}: "
                f"{primary_id} (already processed {prev.get('primaryId')})"
            )
            logger.info(message)
            return PROCESSED_NO_CHANGE, True, message, [], [], [], [], "duplicate_input"
        else:
            # Mark this AGR as processed for this MOD pass
            resources_to_update[agr] = resource_dict

            try:
                process_okay, message, actually_updated, changes, missing_prefix_xrefs, xref_conflicts, xref_additions = \
                    process_update_resource(db_session, resource_dict, agr, mod=mod, writer_mod=writer_mod)
            except Exception as e:
                process_okay = False
                message = f"[db_error] Error updating resource {primary_id} (agr={agr}): {e}"
                logger.error(message)
                failure_kind = "db_error"
                return PROCESSED_FAILED, process_okay, message, [], [], [], [], failure_kind

            logger.info("update primary_id %s db %s (matched via %s)", primary_id, agr, match_type)

            # Add agr + primary_id to each field change for logging
            for change in changes or []:
                change['agr'] = agr
                change['primary_id'] = primary_id

            field_changes = changes or []
            stat = PROCESSED_UPDATED if actually_updated else PROCESSED_NO_CHANGE

            if not process_okay:
                # process_update_resource reported a failure
                failure_kind = "db_error" if "[db_error]" in str(message).lower() else "unknown"
                stat = PROCESSED_FAILED
    else:
        # No existing resource found, create new
        try:
            process_okay, message = process_resource_entry(db_session, resource_dict)
        except Exception as e:
            process_okay = False
            message = f"[db_error] Error creating resource {primary_id}: {e}"
            logger.error(message)
            return PROCESSED_FAILED, process_okay, message, [], [], [], [], "db_error"

        if process_okay:
            if message:
                logger.info(message)
            else:
                logger.error(message)
            stat = PROCESSED_NEW
        else:
            stat = PROCESSED_FAILED
            failure_kind = "db_error" if "[db_error]" in str(message).lower() else "unknown"

    return stat, process_okay, message, field_changes, missing_prefix_xrefs, xref_conflicts, xref_additions, failure_kind


def update_resource(db_session: Session, dqm_entry: dict, db_entry: dict, shared: bool = False) -> Tuple[bool, List[Dict], Optional[str]]:  # noqa: C901
    """
    Update the resource database entry from the sanitized dqm entry.

    Minimal ping-pong fixes:
      - Do NOT patch if field is missing in the DQM entry (treat as "no opinion")
      - Do NOT patch if DQM field value is None (avoid clearing)
      - For list fields (abbreviation_synonyms/title_synonyms/volumes): ADD-ONLY (merge), never remove
      - For shared resources (shared==True): simple fields are fill-only (do not overwrite)
      - Reduce title noise: ignore case/whitespace-only diffs

    :return: Tuple of (actually_updated, field_changes, error_message)
             where field_changes is a list of dicts with 'field', 'old_value', 'new_value'
    """
    global simple_fields
    global list_fields
    global remap_keys

    if not simple_fields:
        simple_fields = ['title', 'isoAbbreviation', 'medlineAbbreviation', 'printISSN',
                         'onlineISSN', 'publisher', 'pages']
    if not list_fields:
        list_fields = ['abbreviationSynonyms', 'titleSynonyms', 'volumes']
    if not remap_keys:
        remap_keys['isoAbbreviation'] = 'iso_abbreviation'
        remap_keys['medlineAbbreviation'] = 'medline_abbreviation'
        remap_keys['printISSN'] = 'print_issn'
        remap_keys['onlineISSN'] = 'online_issn'
        remap_keys['abbreviationSynonyms'] = 'abbreviation_synonyms'
        remap_keys['titleSynonyms'] = 'title_synonyms'
        remap_keys['crossReferences'] = 'cross_references'
        remap_keys['editorsOrAuthors'] = 'editors'

    agr = db_entry['curie']
    update_json = dict()
    field_changes: List[Dict] = []

    # ---- simple fields ----
    for field_camel in simple_fields:
        field_snake = camel_to_snake(field_camel, remap_keys)

        # Treat missing field as "no opinion"
        if field_camel not in dqm_entry:
            continue

        dqm_value = dqm_entry.get(field_camel)
        if dqm_value is None:
            continue

        db_value = db_entry.get(field_snake)

        # Avoid churn on title case/whitespace differences
        if field_snake == 'title' and isinstance(dqm_value, str) and isinstance(db_value, str):
            if dqm_value.strip().lower() == db_value.strip().lower():
                continue

        if shared:
            # shared: fill-only (do not overwrite non-empty)
            if (db_value is None or db_value == '') and dqm_value not in (None, ''):
                logger.info(f"patch(shared-fill) {agr} field {field_snake} from db {db_value} to dqm {dqm_value}")
                update_json[field_snake] = dqm_value
                field_changes.append({'field': field_snake, 'old_value': db_value, 'new_value': dqm_value})
        else:
            if dqm_value != db_value:
                logger.info(f"patch {agr} field {field_snake} from db {db_value} to dqm {dqm_value}")
                update_json[field_snake] = dqm_value
                field_changes.append({'field': field_snake, 'old_value': db_value, 'new_value': dqm_value})

    # ---- list fields (ADD-ONLY) ----
    for field_camel in list_fields:
        if field_camel not in dqm_entry:
            continue
        if dqm_entry.get(field_camel) is None:
            continue

        field_snake = camel_to_snake(field_camel, remap_keys)
        db_values = db_entry.get(field_snake) or []
        dqm_values = dqm_entry.get(field_camel) or []

        merged = _merge_case_insensitive(db_values, dqm_values)
        if merged != db_values:
            logger.info(f"patch(add-only) {agr} field {field_snake} from db {db_values} to merged {merged}")
            update_json[field_snake] = merged
            field_changes.append({'field': field_snake, 'old_value': db_values, 'new_value': merged})

    if not update_json:
        return False, field_changes, None

    try:
        rowcount = db_session.query(ResourceModel).filter_by(curie=agr).update(update_json)
        db_session.commit()
        if rowcount != 1:
            mess = f"Update affected {rowcount} rows for curie={agr}; expected 1."
            logger.error(mess)
            return False, field_changes, mess
        logger.info("The resource row for curie = " + agr + " has been updated.")
        return True, field_changes, None
    except Exception as e:
        db_session.rollback()
        mess = f"An error occurred when updating resource row for curie={agr}: {e}"
        logger.error(mess)
        return False, field_changes, mess


def process_update_resource(db_session, dqm_entry, agr, mod: str = "", writer_mod: str = "") -> Tuple:
    """
    Gets the db entry from the database and converts this to json.
    This is then used in the update_resource function to update the
    database. Its cross references and editors are also updated here.

    Ping-pong fixes:
      - If NLM-controlled and running in a MOD pass (mod != ''), skip ALL field updates
      - Also skip reporting/counting for NLM-controlled resources during MOD pass:
          * still add xrefs to DB
          * but do not return xref additions/conflicts/missing-prefix lists (so they don't show in reports)
          * and do not mark actually_updated True because of xref additions
      - For shared resources (non-NLM), last-mod-wins for field updates

    NOTE: Use xref-cache based NLM detection (agr_has_xref_of_prefix) because db_entry json often
          does not include relationship fields like cross_references.

    :return: Tuple of (okay, error_message, actually_updated, field_changes,
             missing_prefix_xrefs, xref_conflicts, xref_additions)
    """
    try:
        db_entry = db_session.query(ResourceModel).filter(ResourceModel.curie == agr).one()
    except NoResultFound:
        return False, f"Unable to find unique resource with curie {agr}.", False, [], [], [], [], "db_error"

    db_entry = jsonable_encoder(db_entry)

    shared = _is_shared_resource(db_entry, dqm_entry)

    mod_prefixes = ["FB", "ZFIN", "MGI", "RGD", "WB", "SGD", "XB"]
    mods_present = [p for p in mod_prefixes if agr_has_xref_of_prefix(agr, p)]
    shared = shared or (len(mods_present) >= 2)

    # Reliable NLM detection:
    #   - cache lookup via load_xref_data + agr_has_xref_of_prefix
    #   - plus best-effort local detection based on serialized entry
    nlm_controlled = agr_has_xref_of_prefix(agr, "NLM") or has_nlm_id(db_entry, dqm_entry)

    okay = True
    error_message = ""
    actually_updated = False
    field_changes: List[Dict] = []
    missing_prefix_xrefs: List[str] = []
    xref_conflicts: List[str] = []
    xref_additions: List[Dict] = []

    # If NLM-controlled during MOD pass, skip all field patches (NLM/PubMed owns titles, etc.)
    if nlm_controlled and mod:
        logger.info("NLM-controlled: skip MOD field updates for %s (%s)", agr, dqm_entry.get("primaryId"))
    else:
        # last-mod-wins for shared resources (non-NLM)
        skip_shared_field_updates = shared and writer_mod and mod and (mod != writer_mod)
        if skip_shared_field_updates:
            pass
        else:
            actually_updated, field_changes, update_err = update_resource(db_session, dqm_entry, db_entry, shared=shared)
            if update_err:
                okay = False
                error_message = update_err
                actually_updated = False

    # --- Xrefs (add-only) ---
    if 'crossReferences' in dqm_entry:
        if nlm_controlled and mod:
            # Still add xrefs, but do not report/count them for NLM-controlled resources in MOD pass
            okay_x, err_x, _, _, _ = compare_xref(agr, db_entry['resource_id'], dqm_entry, report=False)
            if not okay_x:
                okay = False
                error_message = (error_message + " " + err_x).strip()
        else:
            okay_x, err_x, missing_prefix_xrefs, xref_conflicts, xref_additions = compare_xref(
                agr, db_entry['resource_id'], dqm_entry, report=True
            )
            if not okay_x:
                okay = False
                error_message = (error_message + " " + err_x).strip()

            # Reportable xref adds count as updated
            if xref_additions:
                actually_updated = True

    # Editors (unchanged)
    editors_changed = compare_authors_or_editors(db_entry, dqm_entry, 'editors')
    if editors_changed[0]:
        pass

    return okay, error_message, actually_updated, field_changes, missing_prefix_xrefs, xref_conflicts, xref_additions


def update_resources(db_session, resources_to_update):
    """
    Get the resource from the database, compare to the new resource data.
    Patch simple and list fields.  Add new cross_references and track other
    cases until curators tell us what reports they want.
    This takes 11 minutes to query 34284 resources one by one through the API

    :param  db_session:  db connection
    :param resources_to_update:
    :return:
    NOTE: Not used anymore the entrys are processed as they are recieved and not
          collected to do at the end. DELETE theis function once certain.
          exit(-1) added to check for this.
    """
    exit(-1)
    for agr in resources_to_update:
        process_update_resource(db_session, resources_to_update[agr], agr)


def camel_to_snake(field_camel, remap_keys):
    """

    :param field_camel:
    :param remap_keys:
    :return:
    """
    field_snake = field_camel
    if field_camel in remap_keys:
        field_snake = remap_keys[field_camel]
    return field_snake


def compare_xref(agr, resource_id, dqm_entry, report: bool = True):
    """
    We're running dqm resource updates mod by mod instead of aggregating all their data into
    one entry and comparing that to the database. Since we cannot track which mod submission
    an xref went into the database with, we cannot tell which ones should be removed.
    For that reason we're only doing ADD-ONLY of xrefs, and removals will have to be done
    at ABC through the UI.

    When report=False:
      - still performs DB inserts (add_xref)
      - but does NOT populate missing_prefix_xrefs / xref_conflicts / xref_additions
        (caller can suppress reporting for NLM-controlled resources)

    :return: Tuple of (okay, error_message, missing_prefix_xrefs, xref_conflicts, xref_additions)
    """
    okay = True
    error_mess = ""
    missing_prefix_xrefs: List[str] = []
    xref_conflicts: List[str] = []
    xref_additions: List[Dict] = []

    for xref in dqm_entry.get('crossReferences', []):
        curie = xref.get('id')
        if not curie:
            continue

        prefix, identifier, separator = split_identifier(curie, ignore_error=True)
        if prefix is None or identifier is None:
            if report:
                missing_prefix_xrefs.append(curie)
                logger.warning(f"Cross-reference '{curie}' is missing a valid prefix:identifier format")
            continue

        agr_db_from_xref = get_agr_for_xref(prefix, identifier)
        if agr_db_from_xref == agr:
            # Okay just duplication of same data
            logger.info(f"Prefix found {prefix} for {identifier} and agr {agr_db_from_xref}")
        elif agr_has_xref_of_prefix(agr, prefix):
            # Skip - this resource already has an xref of this prefix type
            pass
        elif agr_db_from_xref:
            # Cross-reference already assigned to another resource
            if report:
                xref_conflicts.append(f"{curie} -> {agr_db_from_xref}")
                logger.warning(f"Cross-reference {curie} already assigned to {agr_db_from_xref}, cannot add to {agr}")
        else:
            if is_obsolete(agr, prefix, identifier):
                pass
            else:
                try:
                    logger.info("CREATE: add cross_reference %s to %s", curie, agr)
                    entry = {
                        'curie': identifier,
                        'curie_prefix': prefix,
                        'resource_id': resource_id,
                        'pages': xref.get('pages', [])
                    }
                    add_xref(agr, entry)

                    if report:
                        xref_additions.append({
                            'agr': agr,
                            'primary_id': dqm_entry.get('primaryId', 'unknown'),
                            'xref': curie
                        })

                    logger.info(
                        "The cross_reference row for curie = " + curie + " and resource_curie = " + agr + " has been added into database."
                    )
                except Exception as e:
                    okay = False
                    mess = (
                        f"An error occurred when adding cross_reference row for curie = {curie} "
                        f"and resource_curie = {agr} Error:{e}"
                    )
                    logger.info(mess)
                    error_mess += mess

    return okay, error_mess, missing_prefix_xrefs, xref_conflicts, xref_additions


def compare_list(db_entry, dqm_entry, field_camel, remap_keys):
    """
    compare case-insensitive if two lists contain the same values from db and dqm dicts

    NOTE: This function is no longer used for list patching (we use add-only merge),
          but keep it for backward compatibility / debugging.
    """
    field_snake = camel_to_snake(field_camel, remap_keys)
    db_values = []
    dqm_values = []
    if field_snake in db_entry:
        if db_entry[field_snake] is not None:
            db_values = db_entry[field_snake]
    lower_db_values = [i.lower() for i in db_values]
    if field_camel in dqm_entry:
        if dqm_entry[field_camel] is not None:
            dqm_values = dqm_entry[field_camel]
    lower_dqm_values = [i.lower() for i in dqm_values]
    if set(lower_db_values) == set(lower_dqm_values):
        return False, None, None
    else:
        return True, dqm_values, db_values, field_snake
