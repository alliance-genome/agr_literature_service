"""SCRUM-6764: nxml2md links the Markdown it makes to the MODs of its source,
not to no MOD (which made a MOD's converted PDF readable by every MOD)."""
from agr_literature_service.lit_processing.oneoff_scripts.nxml2md import source_mod_abbreviations


class _FakeDb:
    def __init__(self, rows):
        self.rows = rows
        self.params = None

    def execute(self, _query, params):
        self.params = params
        return iter(self.rows)


def test_mod_supplied_source_gives_its_mods():
    db = _FakeDb([(1, "SGD"), (1, "WB"), (2, "FB")])
    assert source_mod_abbreviations(db, 7, "25978602") == ["FB", "SGD", "WB"]
    assert db.params == {"reference_id": 7, "display_name": "25978602"}


def test_any_no_mod_link_keeps_the_markdown_open():
    assert source_mod_abbreviations(_FakeDb([(1, "FB"), (2, None)]), 7, "x") == [None]


def test_no_source_row_keeps_the_markdown_open():
    assert source_mod_abbreviations(_FakeDb([]), 7, "x") == [None]
