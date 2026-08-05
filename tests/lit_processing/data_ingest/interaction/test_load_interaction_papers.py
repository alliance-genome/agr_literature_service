import gzip
from unittest.mock import patch

from agr_literature_service.lit_processing.data_ingest.interaction import load_interaction_papers as lip

MODULE = "agr_literature_service.lit_processing.data_ingest.interaction.load_interaction_papers"


def _write_gz(path, lines):
    with gzip.open(path, "wt") as f:
        f.write("\n".join(lines) + "\n")


def _row(pmid_field, source_col12):
    # Build a >12 column interaction row; col 8 = publication id, col 12 = source.
    items = ["x"] * 15
    items[8] = pmid_field
    items[12] = source_col12
    return "\t".join(items)


class TestParseInteractionSources:
    def test_extracts_single_source(self):
        assert lip.parse_interaction_sources('psi-mi:"MI:0463"(biogrid)') == ["biogrid"]

    def test_multi_source_cell_returns_every_source(self):
        field = 'psi-mi:"MI:0463"(biogrid)|psi-mi:"MI:0469"(intact)'
        assert lip.parse_interaction_sources(field) == ["biogrid", "intact"]

    def test_no_parenthetical_returns_empty(self):
        assert lip.parse_interaction_sources("no-parentheses-here") == []

    def test_structural_characters_dropped(self):
        # tokens that would make garbage users.id values are dropped
        assert lip.parse_interaction_sources("foo(psi-mi:MI:0463)") == []
        assert lip.parse_interaction_sources("foo()") == []

    def test_nested_parenthetical_dropped(self):
        # a stray "(" leaked by the non-greedy capture must not become an id
        assert lip.parse_interaction_sources('psi-mi:"MI:1106"(pdbe (ebi))') == []


class TestExtractPmids:
    def test_counts_rows_per_source_per_pmid(self, tmp_path):
        gz = tmp_path / "INTERACTION-MOL_WB.tsv.gz"
        _write_gz(str(gz), [
            "# a comment line",
            _row("pubmed:111", 'psi-mi:"MI:0463"(biogrid)'),
            _row("pubmed:111", 'psi-mi:"MI:0463"(biogrid)'),
            _row("pubmed:111", 'psi-mi:"MI:0469"(IntAct)'),
            _row("222", 'psi-mi:"MI:0471"(MINT)'),
            "\t".join(["x"] * 15),  # no valid pmid -> ignored
        ])
        with patch(f"{MODULE}.file_path", str(tmp_path) + "/"), \
                patch(f"{MODULE}.download_file"):
            file_name, all_pmids, counts = lip.extract_pmids(None, "WB", "MOL")

        assert file_name == "INTERACTION-MOL_WB.tsv.gz"
        assert all_pmids == {"111", "222"}
        assert counts["111"] == {"biogrid": 2, "IntAct": 1}
        assert counts["222"] == {"MINT": 1}

    def test_multi_source_row_counts_each_source(self, tmp_path):
        gz = tmp_path / "INTERACTION-MOL_WB.tsv.gz"
        _write_gz(str(gz), [
            _row("pubmed:111", 'psi-mi:"MI:0463"(biogrid)|psi-mi:"MI:0469"(HPIDb)'),
        ])
        with patch(f"{MODULE}.file_path", str(tmp_path) + "/"), \
                patch(f"{MODULE}.download_file"):
            _, _, counts = lip.extract_pmids(None, "WB", "MOL")
        assert counts["111"] == {"biogrid": 1, "HPIDb": 1}

    def test_same_source_twice_in_a_row_counts_once(self, tmp_path):
        gz = tmp_path / "INTERACTION-MOL_WB.tsv.gz"
        _write_gz(str(gz), [
            _row("pubmed:111", 'psi-mi:"MI:0463"(biogrid)|psi-mi:"MI:0463"(biogrid)'),
        ])
        with patch(f"{MODULE}.file_path", str(tmp_path) + "/"), \
                patch(f"{MODULE}.download_file"):
            _, _, counts = lip.extract_pmids(None, "WB", "MOL")
        assert counts["111"] == {"biogrid": 1}

    def test_unparseable_source_is_not_counted(self, tmp_path):
        gz = tmp_path / "INTERACTION-MOL_WB.tsv.gz"
        _write_gz(str(gz), [_row("pubmed:111", "no-parentheses-here")])
        with patch(f"{MODULE}.file_path", str(tmp_path) + "/"), \
                patch(f"{MODULE}.download_file"):
            _, all_pmids, counts = lip.extract_pmids(None, "WB", "MOL")

        assert all_pmids == {"111"}
        assert "111" not in counts


class TestComposeReportTitle:
    def test_standard_mod(self):
        assert lip.compose_report_title("INTERACTION-GEN_SGD.tsv.gz") == "SGD: INTERACTION-GEN "

    def test_xenbase_dataset(self):
        assert lip.compose_report_title("INTERACTION-MOL_XBXL.tsv.gz") == "XB: INTERACTION-MOL XBXL"


class TestAppendCurationStatusMessage:
    def test_none_result_is_noop(self):
        assert lip.append_curation_status_message("m", None) == "m"

    def test_folds_counts_into_message(self):
        result = {"topic": "ATP:0000069", "added": 2, "updated": 1, "skipped": 3}
        msg = lip.append_curation_status_message("m", result)
        assert msg.startswith("m")
        assert "ATP:0000069" in msg and "2 marked complete" in msg \
            and "1 blank status filled" in msg and "3 already had a status" in msg


class TestLoadDataMarksCurationStatus:
    @patch(f"{MODULE}.mark_interaction_curation_complete")
    @patch(f"{MODULE}.check_pmids_and_compose_message", return_value="msg")
    @patch(f"{MODULE}.get_mod_papers", return_value=(set(), set()))
    @patch(f"{MODULE}.retrieve_all_pmids", return_value=["111"])
    @patch(f"{MODULE}.extract_pmids")
    @patch(f"{MODULE}.clean_up_tmp_directories")
    @patch(f"{MODULE}.set_global_user_id")
    @patch(f"{MODULE}.create_postgres_session")
    def test_no_new_pmids_still_marks_and_reports(
            self, mock_session, mock_user, mock_clean, mock_extract,
            mock_retrieve, mock_mod_papers, mock_check, mock_mark):
        mock_extract.return_value = (
            "INTERACTION-MOL_WB.tsv.gz", {"111"}, {"111": {"biogrid": 3}}
        )
        mock_mark.return_value = {"topic": "ATP:0000069", "added": 1,
                                  "updated": 0, "skipped": 0}
        message = lip.load_data("WB", "MOL", set(), "")

        # single automation user for every write this script makes
        assert mock_user.call_args.args[1] == "load_interactions"
        # corpus fetched once and shared with both the report and the marking
        mock_mod_papers.assert_called_once()
        mock_mark.assert_called_once()
        assert mock_mark.call_args.args[4] == {"111": {"biogrid": 3}}
        assert mock_mark.call_args.args[5] == set()  # shared in_corpus_set
        # counts reach the Slack report
        assert message.startswith("msg")
        assert "1 marked complete" in message
