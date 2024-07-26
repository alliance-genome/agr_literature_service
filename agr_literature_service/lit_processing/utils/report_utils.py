import re
from os import environ
import logging
from datetime import datetime
from agr_literature_service.lit_processing.utils.email_utils import send_email

logging.basicConfig(format='%(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def send_report(email_subject, email_message, email=None):

    email_recipients = email
    if email_recipients is None:
        if environ.get('CRONTAB_EMAIL'):
            email_recipients = environ['CRONTAB_EMAIL']
        else:
            return

    sender_email = None
    if environ.get('SENDER_EMAIL'):
        sender_email = environ['SENDER_EMAIL']
    sender_password = None
    if environ.get('SENDER_PASSWORD'):
        sender_password = environ['SENDER_PASSWORD']
    reply_to = sender_email
    if environ.get('REPLY_TO'):
        reply_to = environ['REPLY_TO']

    (email_status, message) = send_email(email_subject, email_recipients, email_message,
                                         sender_email, sender_password, reply_to)
    if email_status == 'error':
        logger.info("Failed sending email to " + email_recipients + ": " + message + "\n")


def send_data_export_report(status, email, mod, email_message):

    email_subject = None
    if status == 'SUCCESS':
        email_subject = "The " + mod + " Reference json file is ready for download"
    else:
        email_subject = "Error Report for " + mod + " Reference download"

    send_report(email_subject, email_message, email)


def _report_unparsable_date_published(bad_date_published, is_pubmed):

    email_message = ''
    i = 0
    id_type = "paper(s)"
    if is_pubmed:
        id_type = "PMID(s)"
    for ID in bad_date_published:
        id_with_prefix = ID
        if is_pubmed:
            id_with_prefix = "PMID:" + str(ID)
        i += 1
        if i == 1:
            logger.info("Following " + id_type + " have unparsable date_published field:")
            email_message = email_message + "<p>Following " + id_type + " have unparsable date_published field:<p>"
        logger.info(id_with_prefix + ": " + str(bad_date_published[ID]))
        email_message = email_message + id_with_prefix + ": " + str(bad_date_published[ID]) + "<br>"
    if i > 0:
        email_message = email_message + "<p>"

    return email_message


def send_pubmed_search_report(pmids4mod, mods, log_path, log_url, not_loaded_pmids4mod, bad_date_published):

    all_pmids = pmids4mod.get('all')
    if all_pmids is None:
        return

    email_subject = "PubMed Paper Search Report"
    email_message = ""
    if len(all_pmids) == 0:
        email_message = "No new papers from PubMed Search"
    else:
        log_file = log_path + "new_papers.log"
        fw = open(log_file, "w")
        message = "Total " + str(len(all_pmids)) + " new PubMed paper(s) have been added into database"
        fw.write(message + "\n\n")
        email_message = "<h3>" + message + "</h3>"

        rows = ""
        for mod in mods:
            pmids = pmids4mod.get(mod)
            if pmids is None:
                continue
            rows = rows + "<tr><th width='80'>" + mod + ":</th><td>" + str(len(pmids)) + "</td></tr>"
            fw.write(mod + ": " + str(len(pmids)) + "\n")

        email_message = email_message + "<table></tbody>" + rows + "</tbody></table>"

        rows = ''
        for mod in mods:
            if mod not in not_loaded_pmids4mod:
                continue
            not_loaded_pmids = not_loaded_pmids4mod[mod]
            for not_loaded_pmid_row in not_loaded_pmids:
                (pmid_new, xref_id, pmid_in_db) = not_loaded_pmid_row
                if xref_id.startswith('PMC'):
                    rows = rows + "<tr><th width='80'>" + mod + ":</th><td><b>PMID:" + pmid_new + "</b> was not added since its PMCID:" + xref_id + " already exists. This PMCID is associated with PMID:" + pmid_in_db + " in the database.</td></tr>"
                else:
                    rows = rows + "<tr><th width='80'>" + mod + ":</th><td><b>PMID:" + pmid_new + "</b> was not added since its DOI:" + xref_id + " already exists. This DOI is associated with PMID:" + pmid_in_db + " in the database.</td></tr>"
        if rows != '':
            email_message = email_message + "<p><strong>Following new PMID(s) were not added to ABC from PubMed Search</strong><p>"
            email_message = email_message + "<table></tbody>" + rows + "</tbody></table>"

        msg = _report_unparsable_date_published(bad_date_published, True)
        email_message = email_message + msg

        if log_url:
            email_message = email_message + "<p>Log file(s) are available at " + "<a href=" + log_url + ">" + log_url + "</a><p>"
        else:
            email_message = email_message + "<p>Log file(s) are available at " + log_path

        fw.write("\n")
        for mod in mods:
            pmids = pmids4mod.get(mod)
            if pmids is None:
                continue
            pmids_to_report = list(map(lambda x: 'PMID:' + x, pmids))
            fw.write("New papers for " + mod + ":\n")
            fw.write("\n".join(pmids_to_report) + "\n\n")

    send_report(email_subject, email_message)


def send_dqm_loading_report(mod, rows_to_report, missing_papers_in_mod, agr_to_title, bad_date_published, mod_ids_used_in_resource, clashed_pmids, log_path, curie_to_reftypesConflict):  # noqa: C901

    log_url = None
    if environ.get('LOG_URL'):
        log_url = environ['LOG_URL'] + "dqm_load/"

    email_subject = mod + " DQM Loading Report"
    email_message = "<h3>" + mod + " DQM Loading Report</h3>"

    if len(rows_to_report) > 0:
        rows = ''
        i = 0
        (dbid, error) = rows_to_report[0]
        width = len(dbid) * 11

        for x in rows_to_report:
            i += 1
            if i >= 15:
                break
            (dbid, error) = x
            rows = rows + "<tr><th style='text-align:left' width='" + str(width) + "'>" + dbid + ":</th><td>" + error + "</td></tr>"
        email_message = email_message + "<table></tbody>" + rows + "</tbody></table>"

    if log_url:
        email_message = email_message + "<p>Loading log file is available at " + "<a href=" + log_url + ">" + log_url + "</a><p>"
    else:
        email_message = email_message + "<p>Loading log file is available at " + log_path

    if len(mod_ids_used_in_resource) > 0:
        rows = ''
        i = 0
        (dbid, XREF_ID) = mod_ids_used_in_resource[0]
        width = len(dbid) * 11
        for x in mod_ids_used_in_resource:
            (dbid, XREF_ID) = x
            rows = rows + "<tr><th style='text-align:left' width='" + str(width) + "'>" + dbid + ":</th><td>" + XREF_ID + "</td></tr>"
        email_message = email_message + "<p><b>Following papers are not loaded since one of their XREF IDs is associated with a RESOURCE</b></p>"
        email_message = email_message + "<table></tbody>" + rows + "</tbody></table>"

    # missing_papers_in_mod, agr_to_title, log_path
    if len(missing_papers_in_mod) > 0:

        log_file = mod + "_papers_marked_as_out_corpus.log"

        missing_papers_in_mod_log_file = log_path + log_file

        fw = open(missing_papers_in_mod_log_file, "w")

        fw.write("ARG_curie\tPMID\tMOD_ID\tTitle\n")

        email_message = email_message + "<p><p><b>Following paper(s) have been marked as out of corpus since they are not in the current " + mod + " DQM file</b><p>"

        rows = ''
        i = 0
        dbid_width = None
        agr_width = 270
        pmid_width = 140
        for x in missing_papers_in_mod:
            (dbid, agr_curie, pmid) = x
            title = agr_to_title.get(agr_curie, '')
            fw.write(agr_curie + "\t" + str(pmid) + "\t" + dbid + "\t" + title + "\n")
            i += 1
            if i < 6:
                if dbid_width is None:
                    dbid_width = len(dbid) * 11
                rows = rows + "<tr><td style='text-align:left' width='" + str(agr_width) + "'>" + agr_curie + "</td><td width='" + str(pmid_width) + "'>" + str(pmid) + "</td><td width='" + str(dbid_width) + "'>" + dbid + "</td><td width='400'>" + title + "</td></tr>"

        fw.close()

        rows = "<tr><th style='text-align:left' width='" + str(agr_width) + "'>AGR curie</th><th width='" + str(pmid_width) + "'>PMID</th><th width='" + str(dbid_width) + "'>MOD ID</th><th width='400'>Title</th></tr>" + rows

        email_message = email_message + "<table></tbody>" + rows + "</tbody></table>"

        msg = _report_unparsable_date_published(bad_date_published, False)
        email_message = email_message + msg

        if log_url:
            log_url = log_url + log_file
            email_message = email_message + "<p>The full list of newly marked out of corpus papers is available at " + "<a href=" + log_url + ">" + log_url + "</a><p>"
        else:
            log_path = log_path + log_file
            email_message = email_message + "<p>The full list of newly marked out of corpus papers is available at " + log_path

    # for PMIDs that are in both MOD false positive list and MOD current dqm reference file
    if len(clashed_pmids) > 0:
        log_file = mod + "_clashed_PMIDs.log"
        clashed_pmid_log_file = log_path + log_file
        fw = open(clashed_pmid_log_file, "w")
        fw.write("The PMIDs that are in both MOD false positive list and in current MOD DQM reference file:\n")
        fw.write("\n".join(clashed_pmids) + "\n")
        fw.close()
        email_message = email_message + "<p><p><b>" + str(len(clashed_pmids)) + " PMID(s) that are in both " + mod + " false positive list and in " + mod + " current DQM submission</b><p>"
        if log_url:
            log_url = log_url + log_file
            email_message = email_message + "<p>The clashed PMID list is available at " + "<a href=" + log_url + ">" + log_url + "</a><p>"
        else:
            log_path = log_path + log_file
            email_message = email_message + "<p>The clashed PMID list is available at " + log_path

    i = 0
    for curie in curie_to_reftypesConflict:
        if curie_to_reftypesConflict[curie] and len(curie_to_reftypesConflict[curie]) > 0:
            i += 1
            if i == 1:
                email_message = email_message + "<p><p><b>Papers with conflict MOD Reference Types</b></p></p>"
            email_message = email_message + f"<strong>{curie}:</strong> {curie_to_reftypesConflict[curie]}<br>"

    ## only send report once a week - on Saturday
    now = datetime.now()
    if now.weekday() == 5:
        # it returns the day of the week as an integer, where Monday is 0.
        send_report(email_subject, email_message)


def write_log_and_send_pubmed_no_update_report(fw, mod, email_subject):

    logger.info("No new update in PubMed.")
    fw.write("No new update in PubMed.\n")
    logger.info("DONE!\n")
    fw.write("DONE!\n")

    if mod is None:
        return

    email_message = None
    if mod:
        if mod == "NONE":
            email_message = "No new update found in PubMed for the papers that are not associated with a mod"
        else:
            email_message = "No new update found in PubMed for " + mod + " papers"
    else:
        email_message = "No new update found in PubMed"
    email_message = "<strong>" + email_message + "</strong>"

    send_report(email_subject, email_message)


def check_pubmed_status_change(text):

    pattern = r'pubmed_publiction_status:\s*\'(ppublish|epublish)\'\s*to\s*\'(ppublish|epublish)\''

    if re.search(pattern, text):
        return True
    else:
        return False


def write_log_and_send_pubmed_update_report(fw, mod, field_names_to_report, update_log, bad_date_published, authors_with_first_or_corresponding_flag, not_found_xml_list, log_url, log_dir, email_subject, pmids_with_pub_status_changed):  # noqa: C901

    message = None
    if mod:
        if mod == 'NONE':
            message = "Updating Summary for pubmed papers that are not associated with a mod..."
        else:
            message = "Updating Summary for " + mod + " pubmed papers..."
    else:
        message = "Updating Summary..."

    fw.write(message + "\n")
    logger.info(message)
    email_message = "<h3>" + message + "</h3>"

    for field_name in field_names_to_report:
        if field_name == 'pmids_updated':
            continue
        logger.info("Paper(s) with " + field_name + " updated:" + str(update_log[field_name]))
        fw.write("Paper(s) with " + field_name + " updated:" + str(update_log[field_name]) + "\n")
        email_message = email_message + "Paper(s) with <b>" + field_name + "</b> updated:" + str(update_log[field_name]) + "<br>"

    pmids_updated = list(set(update_log['pmids_updated']))

    if len(pmids_updated) == 0:
        email_message = email_message + "<strong>No papers updated.</strong><p>"
    else:
        if len(pmids_updated) <= 100:
            email_message = email_message + "<strong>Total " + str(len(pmids_updated)) + " pubmed paper(s) have been updated</strong>. PMID(s):<br>" + ", ".join(pmids_updated) + "<p>"
        else:
            email_message = email_message + "<strong>Total " + str(len(pmids_updated)) + " pubmed paper(s) have been updated</strong>. PMID(s):<br>" + ", ".join(pmids_updated[0:100]) + "<br>See log file for the full updated PMID list and update details.<p>"
        if log_url:
            email_message = email_message + "<b>The log files are available at: </b><a href=" + log_url + ">" + log_url + "</a><p>"

        fw.write("Total " + str(len(pmids_updated)) + " pubmed paper(s) have been updated. See the following PMID list:\n" + ", ".join(pmids_updated) + "\n")

    msg = _report_unparsable_date_published(bad_date_published, True)
    email_message = email_message + msg

    if len(authors_with_first_or_corresponding_flag) > 0:

        logger.info("The following PMID(s) with author info updated in PubMed, but they have first_author or corresponding_author flaged in the database")
        fw.write("The following PMID(s) with author info updated in PubMed, but they have first_author or corresponding_author flaged in the database\n")
        email_message = email_message + "<b>The following PMID(s) with author info updated in PubMed, but they have first_author or corresponding_author flaged in the database</b><p>"

        for x in authors_with_first_or_corresponding_flag:
            (paper_id, name, first_author, corresponding_author) = x
            logger.info(paper_id + ": name = " + name + ", " + first_author + ", " + corresponding_author)
            fw.write(paper_id + ": name = " + name + ", " + first_author + ", " + corresponding_author + "\n")
            email_message = email_message + paper_id + ": name =" + name + ", first_author=" + first_author + ", corresponding_author=" + corresponding_author + "<br>"

    if len(not_found_xml_list) > 0:
        i = 0
        for pmid in not_found_xml_list:
            if not str(pmid).isdigit():
                continue
            if i == 0:
                logger.info("The following PMID(s) are missing while updating pubmed data")
                fw.write("The following PMID(s) are missing while updating pubmed data")
                email_message = email_message + "<p><b>The following PMID(s) are missing while updating pubmed data</b>:<p>"
            i += 1
            logger.info("PMID:" + str(pmid))
            fw.write("PMID:" + str(pmid) + "\n")
            email_message = email_message + "PMID:" + str(pmid) + "<br>"
        email_message = email_message + "<p>"

    changes_for_published_papers = {}
    for pub_status_change in pmids_with_pub_status_changed:
        data_change_for_status = pmids_with_pub_status_changed[pub_status_change]
        if check_pubmed_status_change(pub_status_change):
            for pmid, values_list in data_change_for_status.items():
                changes_for_published_papers[pmid] = values_list

        i = 0
        for pmid in data_change_for_status:
            if i == 0:
                logger.info(f"Updates in Journal, Volume, or Issue Fields for Papers Changing {pub_status_change}:")
                fw.write(f"Updates in Journal, Volume, or Issue Fields for Papers Changing {pub_status_change}:\n")
            i += 1
            data = data_change_for_status[pmid]
            for (colName, oldVal, newVal) in data:
                displayColName = "journal" if colName == 'resource_id' else colName
                logger.info(f"PMID:{pmid}: {displayColName} from '{oldVal}' to '{newVal}'")
                fw.write(f"PMID:{pmid}: {displayColName} from '{oldVal}' to '{newVal}'\n")

    i = 0
    rows = ''
    for pmid in sorted(changes_for_published_papers):
        if i == 0:
            email_message += "<p><b>Updates in Journal, Author, Volume, or Issue Fields for Published Papers:</b><p>"
            rows = "<tr><th style='text-align:left' width=150>PubMed ID</th><th style='text-align:left' width=130>Data Type</th><th style='text-align:left' width=300>Old Value</th><th style='text-align:left' width=300>New Value</th></tr>"
        i += 1
        data = changes_for_published_papers[pmid]
        for (colName, oldVal, newVal) in data:
            displayColName = "journal" if colName == 'resource_id' else colName
            displayColName = displayColName.lower()
            if colName == 'authors' and 'Deleted' in oldVal:
                oldVal = oldVal.replace("Deleted:", "<span style='color:red;'>Deleted:</span>")
                newVal = newVal.replace("Inserted:", "<span style='color:green;'>Inserted:</span>")
            if oldVal or newVal:
                rows += f"<tr><td style='text-align:left'>PMID:{pmid}</td><td style='text-align:left'><strong>{displayColName}</strong></td><td style='text-align:left'>{oldVal}</td><td style='text-align:left'>{newVal}</td></tr>"
    if rows:
        email_message += "<table><tbody>" + rows + "</tbody></table>"

    if mod:
        email_message = email_message + "DONE!<p>"

        send_report(email_subject, email_message)
