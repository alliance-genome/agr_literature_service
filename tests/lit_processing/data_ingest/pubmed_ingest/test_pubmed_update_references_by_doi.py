from agr_literature_service.api.models import CrossReferenceModel, ReferenceModel
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.pubmed_update_references_by_doi \
    import build_doi_search_term, classify_doi_matches, add_pmid_to_existing_papers
from ....fixtures import db  # noqa


class TestBuildDoiSearchTerm:

    def test_strips_curie_prefix_and_quotes_doi(self):
        # Unquoted, PubMed falls back to automatic term mapping when the DOI is
        # not indexed and splits it into "10.1007"[All Fields] AND "11"[Publisher ID],
        # which matched an unrelated paper (SCRUM-6603).
        assert build_doi_search_term("DOI:10.1007/978-3-0348-8853-0_11") == \
            '"10.1007/978-3-0348-8853-0_11"[DOI]'

    def test_accepts_doi_without_prefix(self):
        assert build_doi_search_term("10.1186/s12874-017-0290-z") == '"10.1186/s12874-017-0290-z"[DOI]'


class TestClassifyDoiMatches:

    def test_accepts_pmid_whose_doi_matches(self):
        candidates = [(983878, "DOI:10.17912/micropub.biology.001013", "38152060")]
        pmid_to_dois = {"38152060": {"10.17912/micropub.biology.001013"}}

        to_add, to_merge, duplicates, rejected = classify_doi_matches(candidates, pmid_to_dois, {})

        assert to_add == [(983878, "PMID:38152060")]
        assert to_merge == [] and duplicates == [] and rejected == []

    def test_doi_comparison_ignores_case(self):
        candidates = [(1, "DOI:10.1093/G3JOURNAL/JKAG266", "40000001")]
        pmid_to_dois = {"40000001": {"10.1093/g3journal/jkag266"}}

        to_add, _, _, rejected = classify_doi_matches(candidates, pmid_to_dois, {})

        assert to_add == [(1, "PMID:40000001")]
        assert rejected == []

    def test_rejects_pmid_whose_doi_does_not_match(self):
        # Real false match seen on stage: this DOI came back as the MDPI Sensors paper.
        candidates = [(1036507, "DOI:10.7488/era/1802", "33807664")]
        pmid_to_dois = {"33807664": {"10.3390/s21051802"}}

        to_add, to_merge, duplicates, rejected = classify_doi_matches(candidates, pmid_to_dois, {})

        assert to_add == [] and to_merge == [] and duplicates == []
        assert rejected == [(1036507, "DOI:10.7488/era/1802", "PMID:33807664")]

    def test_rejects_pmid_missing_from_summary(self):
        candidates = [(1, "DOI:10.1/x", "123")]

        to_add, _, _, rejected = classify_doi_matches(candidates, {}, {})

        assert to_add == []
        assert rejected == [(1, "DOI:10.1/x", "PMID:123")]

    def test_pmid_already_in_database_goes_to_merge_report(self):
        candidates = [(5, "DOI:10.1/x", "123")]
        pmid_to_dois = {"123": {"10.1/x"}}

        to_add, to_merge, _, _ = classify_doi_matches(candidates, pmid_to_dois, {"PMID:123": 9})

        assert to_add == []
        assert to_merge == [("DOI:10.1/x", "PMID:123")]

    def test_two_references_resolving_to_same_pmid_are_not_added(self):
        # A PubMed record can carry more than one DOI; if two DOI-only references
        # both verify against it, they are duplicates of each other, and adding
        # the PMID to both violates idx_curie and rolls back the whole run.
        candidates = [(10, "DOI:10.1/a", "123"), (11, "DOI:10.1/b", "123"), (12, "DOI:10.1/c", "456")]
        pmid_to_dois = {"123": {"10.1/a", "10.1/b"}, "456": {"10.1/c"}}

        to_add, to_merge, duplicates, rejected = classify_doi_matches(candidates, pmid_to_dois, {})

        assert to_add == [(12, "PMID:456")]
        assert duplicates == [("DOI:10.1/a", "PMID:123"), ("DOI:10.1/b", "PMID:123")]
        assert to_merge == [] and rejected == []


class TestAddPmidToExistingPapers:

    def test_conflicting_pmid_is_skipped_without_losing_the_others(self, db):  # noqa
        refs = [ReferenceModel(curie=f"AGR:AGR-Reference-000090000{i}", title=f"doi ref {i}")
                for i in range(3)]
        db.add_all(refs)
        db.flush()
        db.add(CrossReferenceModel(reference_id=refs[0].reference_id, curie="PMID:99990001",
                                   curie_prefix="PMID", is_obsolete=False))
        db.commit()

        added = add_pmid_to_existing_papers(db, [(refs[1].reference_id, "PMID:99990001"),
                                                 (refs[2].reference_id, "PMID:99990002")])
        db.commit()

        assert added == ["PMID:99990002"]
        rows = db.query(CrossReferenceModel.reference_id, CrossReferenceModel.curie).filter(
            CrossReferenceModel.curie.in_(["PMID:99990001", "PMID:99990002"])).all()
        assert sorted(rows) == sorted([(refs[0].reference_id, "PMID:99990001"),
                                       (refs[2].reference_id, "PMID:99990002")])
