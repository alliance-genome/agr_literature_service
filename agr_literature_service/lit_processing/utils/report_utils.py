from os import environ

from agr_literature_service.lit_processing.utils.email_utils import send_email


def send_dqm_loading_report(mod, rows_to_report, missing_papers_in_mod, agr_to_title, log_path, logger):

    email_recipients = None
    if environ.get('CRONTAB_EMAIL'):
        email_recipients = environ['CRONTAB_EMAIL']
    sender_email = None
    if environ.get('SENDER_EMAIL'):
        sender_email = environ['SENDER_EMAIL']
    sender_password = None
    if environ.get('SENDER_PASSWORD'):
        sender_password = environ['SENDER_PASSWORD']
    reply_to = sender_email
    if environ.get('REPLY_TO'):
        reply_to = environ['REPLY_TO']
    log_url = None
    if environ.get('LOG_URL'):
        log_url = environ['LOG_URL'] + "dqm_load/"
    if email_recipients is None or sender_email is None:
        return

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

    # missing_papers_in_mod, agr_to_title, log_path
    if len(missing_papers_in_mod) > 0:

        log_file = mod + "_papers_in_ABC_not_in_dqm.log"

        missing_papers_in_mod_log_file = log_path + log_file

        fw = open(missing_papers_in_mod_log_file, "w")

        fw.write("ARG_curie\tPMID\tMOD_ID\tTitle\n")

        email_message = email_message + "<p><p><b>Following papers in ABC with MOD association that are not in the current " + mod + " DQM file</b><p>"

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

        if log_url:
            log_url = log_url + log_file
            email_message = email_message + "<p>The full list of missing papers is available at " + "<a href=" + log_url + ">" + log_url + "</a><p>"
        else:
            log_path = log_path + log_file
            email_message = email_message + "<p>The full list of missing papers is available at " + log_path

    (status, message) = send_email(email_subject, email_recipients,
                                   email_message, sender_email, sender_password, reply_to)
    if status == 'error':
        logger.info("Failed sending email to slack: " + message + "\n")
