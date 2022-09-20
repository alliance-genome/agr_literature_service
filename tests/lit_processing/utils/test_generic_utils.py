from agr_literature_service.lit_processing.utils.generic_utils import split_identifier


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
