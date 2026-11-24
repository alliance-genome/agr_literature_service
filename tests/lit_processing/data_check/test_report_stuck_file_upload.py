"""report_stuck_file_upload lists papers left at "file upload in progress" (SCRUM-6941).

ATP:0000139 is set for any upload that is not a final main PDF and nothing moves a paper on from
it, so papers stay there for years without being converted or classified. The report runs its SQL
against the real schema, so a column rename fails CI instead of the next monthly run (the
SCRUM-6936 failure mode).
"""
from datetime import date, datetime, timedelta

from agr_literature_service.api.models import (CrossReferenceModel, ModCorpusAssociationModel, ModModel,
                                               ReferenceModel, ReferencefileModAssociationModel,
                                               ReferencefileModel, WorkflowTagModel)
from agr_literature_service.api.schemas import ModCorpusSortSourceType
from agr_literature_service.lit_processing.data_check.report_stuck_file_upload import (
    describe_files, get_stuck_file_upload_papers_for_mod, report_lines
)
from agr_literature_service.lit_processing.tests.mod_populate_load import populate_test_mods
from ...fixtures import db  # noqa

IN_PROGRESS = "ATP:0000139"
UPLOADED = "ATP:0000134"


def _mod_id(db, abbreviation):  # noqa
    return db.query(ModModel.mod_id).filter(ModModel.abbreviation == abbreviation).scalar()


def _paper(db, number, atp_id, days_ago, in_corpus=True, pmid=None, pmcid=None):  # noqa
    """A WB paper whose file upload status is atp_id, set days_ago days ago."""
    wb = _mod_id(db, "WB")
    reference = ReferenceModel(curie=f"AGRKB:10100070{number:04d}", category="research_article")
    db.add(reference)
    db.flush()
    db.add(ModCorpusAssociationModel(reference_id=reference.reference_id, mod_id=wb, corpus=in_corpus,
                                     mod_corpus_sort_source=ModCorpusSortSourceType.Dqm_files))
    for curie, prefix in ((pmid, "PMID"), (pmcid, "PMCID")):
        if curie:
            db.add(CrossReferenceModel(curie=curie, curie_prefix=prefix, reference_id=reference.reference_id))
    since = datetime.now() - timedelta(days=days_ago)
    db.add(WorkflowTagModel(reference_id=reference.reference_id, mod_id=wb, workflow_tag_id=atp_id,
                            date_created=since, date_updated=since))
    db.commit()
    return reference


def _curie(number):
    return f"AGRKB:10100070{number:04d}"


def _file(db, reference, name, file_class, extension, status="final", mod=None):  # noqa
    """A file on the paper, for every MOD (PMC) or only for `mod`."""
    referencefile = ReferencefileModel(reference_id=reference.reference_id, display_name=name,
                                       file_class=file_class, file_extension=extension,
                                       file_publication_status=status,
                                       pdf_type="pdf" if extension == "pdf" else None,
                                       md5sum=f"md5-{reference.reference_id}-{name}-{extension}")
    db.add(referencefile)
    db.flush()
    db.add(ReferencefileModAssociationModel(referencefile_id=referencefile.referencefile_id,
                                            mod_id=_mod_id(db, mod) if mod else None))
    db.commit()


def test_reports_papers_stuck_longer_than_the_threshold(db):  # noqa
    populate_test_mods()
    stuck = _paper(db, 1, IN_PROGRESS, 400, pmid="PMID:40632140", pmcid="PMCID:PMC12240580")
    _file(db, stuck, "PMC12240580", "main", "nxml")
    _file(db, stuck, "supp1", "supplement", "pdf")
    _file(db, stuck, "supp2", "supplement", "xlsx")
    _file(db, stuck, "fig1", "figure", "jpg")
    _file(db, stuck, "fig1_thumb", "thumbnail", "gif")  # left out
    _file(db, stuck, "fb_only", "supplement", "pdf", mod="FB")  # another MOD's file: not counted for WB
    older_no_files = _paper(db, 2, IN_PROGRESS, 900)
    _paper(db, 3, IN_PROGRESS, 5)  # too recent
    _paper(db, 4, UPLOADED, 400)  # already has its main PDF
    _paper(db, 5, IN_PROGRESS, 400, in_corpus=False)  # not WB's paper any more

    rows = get_stuck_file_upload_papers_for_mod(db, "WB", min_days=28)

    assert [row["curie"] for row in rows] == [older_no_files.curie, stuck.curie]  # oldest first
    newest = rows[1]
    assert newest["pmid"] == "PMID:40632140"
    assert newest["pmcid"] == "PMCID:PMC12240580"
    assert newest["since"] == date.today() - timedelta(days=400)
    assert newest["days"] == 400
    assert (newest["nxml"], newest["final_main_pdf"], newest["temp_main_pdf"],
            newest["supplements"], newest["figures"], newest["other"]) == (1, 0, 0, 2, 1, 0)
    assert rows[0]["pmid"] is None and rows[0]["supplements"] == 0

    assert get_stuck_file_upload_papers_for_mod(db, "FB", min_days=28) == []
    assert [row["curie"] for row in get_stuck_file_upload_papers_for_mod(db, "WB", min_days=1)] == [
        older_no_files.curie, stuck.curie, _curie(3)]


def test_a_final_main_pdf_left_at_139_is_reported_as_such(db):  # noqa
    # Should not happen (a final main PDF moves the paper to 134), so the report must show it.
    populate_test_mods()
    paper = _paper(db, 6, IN_PROGRESS, 60)
    _file(db, paper, "main", "main", "pdf")
    _file(db, paper, "main_temp", "main", "pdf", status="temp")
    _file(db, paper, "main_merged", "converted_merged_main", "md")

    [row] = get_stuck_file_upload_papers_for_mod(db, "WB", min_days=28)

    assert (row["nxml"], row["final_main_pdf"], row["temp_main_pdf"], row["supplements"], row["figures"],
            row["other"]) == (0, 1, 1, 0, 0, 1)


def test_describe_files():
    assert describe_files(1, 0, 0, 2, 4, 0) == "nXML, 2 supplements, 4 figures"
    assert describe_files(0, 1, 2, 1, 1, 3) == "final main PDF, 2 temp main PDFs, 1 supplement, 1 figure, 3 other"
    assert describe_files(0, 0, 0, 0, 0, 0) == "no files"


def test_report_lines():
    row = {"curie": "AGRKB:1", "pmid": "PMID:1", "pmcid": None, "since": date(2025, 7, 16), "days": 495,
           "nxml": 1, "final_main_pdf": 0, "temp_main_pdf": 0, "supplements": 2, "figures": 4, "other": 0}
    assert report_lines("FB", [row]) == [
        "FB\tAGRKB:1\tPMID:1\t\t2025-07-16\t495\tY\tnXML, 2 supplements, 4 figures"]
