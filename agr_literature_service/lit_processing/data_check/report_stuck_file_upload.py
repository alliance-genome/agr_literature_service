"""Monthly QC report of papers stuck at "file upload in progress" (ATP:0000139), SCRUM-6941.

referencefile_crud.transition_WFT_for_uploaded_file sets ATP:0000139 for any upload that is not a
final main PDF (nXML, supplement, figure, temp PDF), and only a final main PDF or a curator moves
the paper on. Papers can therefore sit there indefinitely without being converted or classified.
This report lists them per MOD, with the files each MOD can see, so curators can upload the PDF
or mark the file unavailable. It changes no statuses.

Writes LOG_PATH/QC/stuck_file_upload_report.log plus a dated copy, one tab-separated line per
paper: mod, curie, pmid, pmcid, at-139-since (YYYY-MM-DD), days, has nXML (Y/N), files.
"""
import argparse
import logging
from datetime import date, datetime, timedelta
from os import environ, path
from shutil import copy
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from agr_literature_service.lit_processing.utils.db_read_utils import get_mod_abbreviations
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

FILE_UPLOAD_IN_PROGRESS = "ATP:0000139"
DEFAULT_MIN_DAYS = 28
REPORT_STEM = "stuck_file_upload_report"


def get_stuck_file_upload_papers_for_mod(db, mod_abbr: str, min_days: int = DEFAULT_MIN_DAYS,
                                         now: Optional[datetime] = None) -> List[Dict[str, Any]]:
    """In-corpus papers of one MOD at ATP:0000139 for at least min_days, oldest first.

    "Since" is the tag's date_updated: a transition changes the existing file upload tag in
    place, so that is when it reached 139 (checked against workflow_tag_version on stage: the
    same day for all 6,251 papers; date_created is not, it is when the tag was first created,
    often at "file needed"). Files count if they are shared (referencefile_mod with no MOD,
    e.g. from PMC) or attached to this MOD. PMC thumbnails are left out: one per figure, and
    they tell a curator nothing.
    """
    now = now or datetime.now()
    sql = text("""
        SELECT
            r.curie,
            (SELECT min(cr.curie) FROM cross_reference cr
              WHERE cr.reference_id = r.id AND cr.curie_prefix = 'PMID' AND NOT cr.obsolete) AS pmid,
            (SELECT min(cr.curie) FROM cross_reference cr
              WHERE cr.reference_id = r.id AND cr.curie_prefix = 'PMCID' AND NOT cr.obsolete) AS pmcid,
            COALESCE(wt.date_updated, wt.date_created) AS since,
            count(rf.id) FILTER (WHERE lower(rf.file_extension) = 'nxml') AS nxml,
            count(rf.id) FILTER (WHERE rf.file_class = 'main' AND lower(rf.file_extension) = 'pdf'
                                   AND rf.file_publication_status = 'final') AS final_main_pdf,
            count(rf.id) FILTER (WHERE rf.file_class = 'main' AND lower(rf.file_extension) = 'pdf'
                                   AND rf.file_publication_status <> 'final') AS temp_main_pdf,
            count(rf.id) FILTER (WHERE rf.file_class = 'supplement') AS supplements,
            count(rf.id) FILTER (WHERE rf.file_class = 'figure') AS figures,
            count(rf.id) FILTER (WHERE lower(rf.file_extension) <> 'nxml'
                                   AND NOT (rf.file_class = 'main' AND lower(rf.file_extension) = 'pdf')
                                   AND rf.file_class NOT IN ('supplement', 'figure', 'thumbnail')) AS other
        FROM workflow_tag wt
        JOIN mod m ON m.id = wt.mod_id
        JOIN reference r ON r.id = wt.reference_id
        JOIN mod_corpus_association mca
          ON mca.reference_id = wt.reference_id AND mca.mod_id = wt.mod_id AND mca.corpus IS TRUE
        LEFT JOIN referencefile rf
          ON rf.reference_id = r.id
         AND EXISTS (SELECT 1 FROM referencefile_mod rfm
                      WHERE rfm.referencefile_id = rf.id AND (rfm.mod_id IS NULL OR rfm.mod_id = m.id))
        WHERE m.abbreviation = :mod_abbr
          AND wt.workflow_tag_id = :atp_id
          AND COALESCE(wt.date_updated, wt.date_created) < :cutoff
        GROUP BY r.id, r.curie, wt.date_updated, wt.date_created
        ORDER BY since, r.curie
    """)
    rows = db.execute(sql, {"mod_abbr": mod_abbr, "atp_id": FILE_UPLOAD_IN_PROGRESS,
                            "cutoff": now - timedelta(days=min_days)}).mappings().fetchall()
    papers = []
    for row in rows:
        paper = dict(row)
        paper["since"] = row["since"].date()
        paper["days"] = (now.date() - paper["since"]).days
        papers.append(paper)
    return papers


def _count(number: int, singular: str, plural: Optional[str] = None) -> str:
    if number == 1:
        return f"1 {singular}"
    return f"{number} {plural or singular + 's'}"


def describe_files(nxml: int, final_main_pdf: int, temp_main_pdf: int, supplements: int, figures: int,
                   other: int) -> str:
    """The files a MOD can see on a paper, e.g. "nXML, 2 supplements, 4 figures"."""
    parts = []
    if nxml:
        parts.append("nXML" if nxml == 1 else f"{nxml} nXML")
    if final_main_pdf:
        parts.append("final main PDF" if final_main_pdf == 1 else f"{final_main_pdf} final main PDFs")
    if temp_main_pdf:
        parts.append("temp main PDF" if temp_main_pdf == 1 else f"{temp_main_pdf} temp main PDFs")
    if supplements:
        parts.append(_count(supplements, "supplement"))
    if figures:
        parts.append(_count(figures, "figure"))
    if other:
        parts.append(f"{other} other")
    return ", ".join(parts) or "no files"


def report_lines(mod_abbr: str, papers: List[Dict[str, Any]]) -> List[str]:
    lines = []
    for paper in papers:
        files = describe_files(paper["nxml"], paper["final_main_pdf"], paper["temp_main_pdf"],
                               paper["supplements"], paper["figures"], paper["other"])
        lines.append("\t".join([mod_abbr, paper["curie"], paper["pmid"] or "", paper["pmcid"] or "",
                                paper["since"].isoformat(), str(paper["days"]),
                                "Y" if paper["nxml"] else "N", files]))
    return lines


def write_report(lines: List[str]):
    log_path = environ.get('LOG_PATH', '.')
    log_file = path.join(log_path, f"QC/{REPORT_STEM}.log")
    datestamp = str(date.today()).replace("-", "")
    log_file_with_datestamp = path.join(log_path, f"QC/{REPORT_STEM}_{datestamp}.log")
    with open(log_file, "w") as f:
        f.write(f"#!date-produced: {datestamp}\n")
        for line in lines:
            f.write(f"{line}\n")
    copy(log_file, log_file_with_datestamp)


def check_data(min_days: int = DEFAULT_MIN_DAYS):
    lines: List[str] = []
    db = create_postgres_session(False)
    try:
        for mod_abbr in get_mod_abbreviations():
            papers = get_stuck_file_upload_papers_for_mod(db, mod_abbr, min_days)
            with_nxml = sum(1 for paper in papers if paper["nxml"])
            logger.info(f"{mod_abbr}: {len(papers)} papers at {FILE_UPLOAD_IN_PROGRESS} for at least "
                        f"{min_days} days, {with_nxml} with an nXML.")
            lines.extend(report_lines(mod_abbr, papers))
    finally:
        db.close()
    write_report(lines)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--min-days", type=int, default=DEFAULT_MIN_DAYS,
                        help=f"only papers at {FILE_UPLOAD_IN_PROGRESS} for at least this many days "
                             f"(default {DEFAULT_MIN_DAYS})")
    check_data(parser.parse_args().min_days)
