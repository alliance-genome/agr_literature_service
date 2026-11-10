"""SCRUM-6764: no-MOD Markdown takes the MODs of its source file, unless the
source is itself open to every MOD or cannot be found."""
import importlib.util
from pathlib import Path
from typing import Any, cast

# The script file is ticket-prefixed, which is not an importable module name;
# load it by path.
_SCRIPT = (
    Path(__file__).resolve().parents[3]
    / "agr_literature_service" / "lit_processing" / "oneoff_scripts"
    / "SCRUM-6764_relink_markdown_mods.py"
)
_spec = importlib.util.spec_from_file_location("relink_markdown_mods", _SCRIPT)
assert _spec is not None and _spec.loader is not None
relink = importlib.util.module_from_spec(_spec)
cast(Any, _spec.loader).exec_module(relink)
script = cast(Any, relink)

FB, WB, SGD = 1, 2, 3


def test_mod_supplied_pdf_relinks_to_its_mod():
    assert script.decide({10: [FB]}) == (script.RELINK, (FB,))


def test_pdf_supplied_by_several_mods_relinks_to_all_of_them():
    assert script.decide({10: [WB, SGD, FB]}) == (script.RELINK, (FB, WB, SGD))


def test_two_matching_sources_relink_to_the_union():
    assert script.decide({10: [FB], 11: [WB]}) == (script.RELINK, (FB, WB))


def test_source_open_to_every_mod_leaves_the_markdown_open():
    assert script.decide({10: [None]}) == (script.OPEN_SOURCE, ())
    assert script.decide({10: [FB, None]}) == (script.OPEN_SOURCE, ())
    assert script.decide({10: [FB], 11: [None]}) == (script.OPEN_SOURCE, ())


def test_source_without_any_link_counts_as_open():
    assert script.decide({10: []}) == (script.OPEN_SOURCE, ())


def test_no_source_leaves_the_markdown_alone():
    assert script.decide({}) == (script.NO_SOURCE, ())


def test_suffix_of():
    assert script.suffix_of("FBrf0233986_tei") == "tei"
    assert script.suffix_of("10543952_1_tei") == "tei"
    assert script.suffix_of("main") == "(none)"
