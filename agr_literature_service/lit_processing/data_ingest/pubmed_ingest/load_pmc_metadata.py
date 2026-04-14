import logging
from os import path
from sqlalchemy import text
from typing import Dict, List, Set

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import \
    create_postgres_session
from agr_literature_service.lit_processing.utils.db_read_utils import get_pmid_to_reference_id_mapping
from agr_literature_service.lit_processing.data_ingest.utils.db_write_utils import \
    insert_referencefile_mod_for_pmc, insert_referencefile
from agr_literature_service.lit_processing.data_ingest.utils.file_processing_utils import \
    classify_pmc_file
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.api.crud.workflow_tag_crud import transition_to_workflow_status

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

infile = "data/pmc_oa_files_uploaded.txt"

# file_class = "supplement"
file_publication_status = "final"
batch_commit_size = 250

# Workflow tag ATP IDs
FILE_UPLOADED_TAG_ATP_ID = "ATP:0000134"  # file uploaded


def build_file_root_mappings(input_file):
    """Build mappings of PMCID to XML and PDF root names for main file identification."""
    pmcid_to_xml_root = {}
    pmcid_to_pdf_roots = {}
    with open(input_file) as f:
        for line in f:
            pieces = line.strip().split("\t")
            if len(pieces) < 4:
                continue
            pmcid = pieces[1]
            file_name_with_suffix = pieces[2]
            if file_name_with_suffix.lower().endswith('.xml'):
                xml_root = file_name_with_suffix.rsplit('.', 1)[0]
                pmcid_to_xml_root[pmcid] = xml_root.lower()
            elif file_name_with_suffix.lower().endswith('.pdf'):
                pdf_root = file_name_with_suffix.rsplit('.', 1)[0].lower()
                if pmcid not in pmcid_to_pdf_roots:
                    pmcid_to_pdf_roots[pmcid] = set()
                pmcid_to_pdf_roots[pmcid].add(pdf_root)
    return pmcid_to_xml_root, pmcid_to_pdf_roots


def determine_file_class(file_name, file_extension, pmcid, pmcid_to_xml_root, pmcid_to_pdf_roots):
    """Determine the file_class for a PMC file based on extension and root name matching."""
    file_class = classify_pmc_file(file_name, file_extension)
    file_root = file_name.lower()

    # Check if this PDF is the main PDF (matches XML root name)
    if file_extension == 'pdf' and pmcid in pmcid_to_xml_root:
        if file_root == pmcid_to_xml_root[pmcid]:
            return 'main'

    # Check if this TXT matches XML root name
    if file_extension == 'txt' and pmcid in pmcid_to_xml_root:
        if file_root == pmcid_to_xml_root[pmcid]:
            return 'txt'

    # Only set nXML for .xml files if there's a matching PDF
    if file_extension == 'xml' and pmcid in pmcid_to_pdf_roots:
        if file_root in pmcid_to_pdf_roots[pmcid]:
            return 'nXML'
        else:
            return 'supplement'

    return file_class


def load_ref_file_metadata_into_db():  # pragma: no cover

    db_session = create_postgres_session(False)
    script_nm = path.basename(__file__).replace(".py", "")
    set_global_user_id(db_session, script_nm)

    ref_files_id_pmc_set = set([row["referencefile_id"] for row in db_session.execute(text(
        "SELECT referencefile_id FROM referencefile_mod WHERE mod_id is null")).mappings().fetchall()])

    ref_file_key_dbid = {}
    ref_file_uniq_filename_set = set()
    for row in db_session.execute(text("SELECT referencefile_id, reference_id, md5sum, display_name, file_extension FROM "
                                       "referencefile")).mappings().fetchall():
        ref_file_key = (row["reference_id"], row["md5sum"])
        ref_file_dbid = row["referencefile_id"]
        uniq_filename = (row["reference_id"], row["display_name"] + "." + row["file_extension"])
        ref_file_key_dbid[ref_file_key] = ref_file_dbid
        ref_file_uniq_filename_set.add(uniq_filename)

    pmid_to_reference_id = get_pmid_to_reference_id_mapping(db_session)

    # First pass: Build mappings to identify main files
    pmcid_to_xml_root, pmcid_to_pdf_roots = build_file_root_mappings(infile)
    logger.info(f"Found {len(pmcid_to_xml_root)} PMCIDs with XML files for main PDF identification")

    # Track reference_ids that get a new main PDF for workflow transitions
    references_with_new_main_pdf: Set[int] = set()

    # Second pass: Process files and load metadata
    with open(infile) as f:
        for line_num, line in enumerate(f):
            if line_num % batch_commit_size == 0:
                db_session.commit()

            pieces = line.strip().split("\t")
            pmid = pieces[0]
            reference_id = pmid_to_reference_id.get(pmid)
            if reference_id is None:
                continue
            pmcid = pieces[1]
            md5sum = pieces[3]
            file_name_with_suffix = pieces[2]
            if (reference_id, file_name_with_suffix) in ref_file_uniq_filename_set:
                file_name_with_suffix = resolve_displayname_conflict(ref_file_uniq_filename_set,
                                                                     file_name_with_suffix,
                                                                     reference_id)
            referencefile_id = None
            if (reference_id, md5sum) in ref_file_key_dbid:
                referencefile_id = ref_file_key_dbid[(reference_id, md5sum)]
                if referencefile_id in ref_files_id_pmc_set:
                    continue

            file_class = None
            if not referencefile_id:
                file_extension = file_name_with_suffix.split(".")[-1].lower()
                file_name = file_name_with_suffix.replace("." + file_extension, "")
                file_class = determine_file_class(file_name, file_extension, pmcid,
                                                  pmcid_to_xml_root, pmcid_to_pdf_roots)
                referencefile_id = insert_referencefile(db_session, pmid, file_class,
                                                        file_publication_status,
                                                        file_name_with_suffix,
                                                        reference_id, md5sum,
                                                        logger)
                # Track if this is a new main PDF
                if referencefile_id and file_class == 'main' and file_extension == 'pdf':
                    references_with_new_main_pdf.add(reference_id)

            if referencefile_id:
                insert_referencefile_mod_for_pmc(db_session, pmid, file_name_with_suffix,
                                                 referencefile_id, logger)
                ref_file_key_dbid[(reference_id, md5sum)] = referencefile_id
                ref_files_id_pmc_set.add(referencefile_id)
                ref_file_uniq_filename_set.add((reference_id, file_name_with_suffix))

        db_session.commit()

    # Transition workflow tags for references with new main PDFs
    if references_with_new_main_pdf:
        logger.info(f"Transitioning workflow tags for {len(references_with_new_main_pdf)} "
                    f"references with new main PDFs...")
        transition_workflow_for_uploaded_pdfs(db_session, references_with_new_main_pdf)

    db_session.close()


def transition_workflow_for_uploaded_pdfs(db_session, reference_ids: Set[int]):
    """
    Transition workflow tags to 'file uploaded' for references that received a new main PDF.

    Args:
        db_session: Database session
        reference_ids: Set of reference_ids that have new main PDFs
    """
    if not reference_ids:
        return

    # Get MOD associations for these references (corpus=True)
    ref_id_list = ','.join(str(rid) for rid in reference_ids)
    rows = db_session.execute(text(f"""
        SELECT mca.reference_id, m.abbreviation
        FROM mod_corpus_association mca
        JOIN mod m ON mca.mod_id = m.mod_id
        WHERE mca.reference_id IN ({ref_id_list})
        AND mca.corpus = TRUE
    """)).fetchall()

    # Group by reference_id (skip AGR as it doesn't have file upload workflow)
    ref_to_mods: Dict[int, List[str]] = {}
    for row in rows:
        ref_id = row[0]
        mod_abbr = row[1]
        if mod_abbr == 'AGR':
            continue  # Skip AGR - no file upload workflow transitions
        if ref_id not in ref_to_mods:
            ref_to_mods[ref_id] = []
        ref_to_mods[ref_id].append(mod_abbr)

    # Get references that already have 'file uploaded' workflow tag
    rows = db_session.execute(text(f"""
        SELECT wt.reference_id, m.abbreviation
        FROM workflow_tag wt
        JOIN mod m ON wt.mod_id = m.mod_id
        WHERE wt.reference_id IN ({ref_id_list})
        AND wt.workflow_tag_id = '{FILE_UPLOADED_TAG_ATP_ID}'
    """)).fetchall()

    already_transitioned = {(row[0], row[1]) for row in rows}

    transition_count = 0
    error_count = 0
    for reference_id, mods in ref_to_mods.items():
        for mod_abbr in mods:
            if (reference_id, mod_abbr) in already_transitioned:
                continue
            try:
                transition_to_workflow_status(db_session, str(reference_id), mod_abbr,
                                              FILE_UPLOADED_TAG_ATP_ID)
                transition_count += 1
                logger.info(f"Transitioned workflow to 'file uploaded' for "
                            f"reference_id={reference_id}, mod={mod_abbr}")
            except Exception as e:
                error_count += 1
                logger.error(f"Failed to transition workflow for reference_id={reference_id}, "
                             f"mod={mod_abbr}: {e}")
                db_session.rollback()

            if transition_count % batch_commit_size == 0:
                db_session.commit()

    db_session.commit()
    logger.info(f"Workflow transitions complete: {transition_count} successful, {error_count} errors")


def resolve_displayname_conflict(ref_file_uniq_filename_set, file_name_with_suffix, reference_id):
    # this function that appends an increasing number to the base file name until
    # a unique file name is found:
    #
    # We first extract the base name from the input "file_name_with_suffix",
    # and initialize a counter to 1. We then append the current count value to the
    # base name to form the new display name.
    #
    # If the (reference_id, file_name_with_suffix) tuple is already present in the
    # "ref_file_uniq_filename_set", we increment the counter, update the display name,
    # and try again. We repeat this process until a unique file name is found.

    file_extension = file_name_with_suffix.split(".")[-1].lower()
    base_name = file_name_with_suffix.replace("." + file_extension, "")
    count = 1
    display_name = base_name + "_" + str(count)
    file_name_with_suffix = display_name + "." + file_extension
    while (reference_id, file_name_with_suffix) in ref_file_uniq_filename_set:
        count += 1
        display_name = base_name + "_" + str(count)
        file_name_with_suffix = display_name + "." + file_extension
    return file_name_with_suffix


if __name__ == "__main__":

    load_ref_file_metadata_into_db()
