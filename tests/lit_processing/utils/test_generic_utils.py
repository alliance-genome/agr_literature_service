from agr_literature_service.lit_processing.utils.generic_utils import split_identifier, xref_lookup_key


class TestGenericUtils:
    def test_split_identifier(self, capfd):
        id_pmid = "PMID:10206683"
        prefix, identifier_processed, separator = split_identifier(id_pmid)
        assert prefix == "PMID"
        assert identifier_processed == "10206683"
        assert separator == ":"

        id_hyphen = "TEST-Resource1"
        prefix, identifier_processed, separator = split_identifier(id_hyphen)
        assert prefix == "TEST"
        assert identifier_processed == "Resource1"
        assert separator == "-"

        id_wrong = "WRONG_001"
        prefix, identifier_processed, separator = split_identifier(id_wrong, ignore_error=True)
        assert prefix is None
        assert identifier_processed is None
        assert separator is None
        print_out = capfd.readouterr()[0]
        assert print_out == ""

        split_identifier(id_wrong, ignore_error=False)
        print_out = capfd.readouterr()[0]
        assert print_out == "Identifier does not contain ':' or '-' characters.\nSplitting identifier is not possible." \
                            "\nIdentifier: WRONG_001\n"

    def test_xref_lookup_key(self):
        # SCRUM-6864: DOIs match regardless of case, other identifiers stay exact
        assert xref_lookup_key("DOI", "10.1534/GENETICS.113.157685") == "10.1534/genetics.113.157685"
        assert xref_lookup_key("DOI", "10.1534/genetics.113.157685") == "10.1534/genetics.113.157685"
        assert xref_lookup_key("WB", "WBPaper00069799") == "WBPaper00069799"
        assert xref_lookup_key("PMCID", "PMC13356707") == "PMC13356707"
        assert xref_lookup_key("DOI", None) is None
