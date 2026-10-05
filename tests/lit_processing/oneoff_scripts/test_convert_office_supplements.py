"""The SCRUM-6589 backfill's pure helpers: year parsing and the publication
year precedence it shares with the classifier trainer."""
import importlib.util
from pathlib import Path
from typing import Any, cast
from unittest.mock import MagicMock

import pytest

# Ticket-prefixed script name is not an importable module name: load by path.
_SCRIPT = (
    Path(__file__).resolve().parents[3]
    / "agr_literature_service" / "lit_processing" / "oneoff_scripts"
    / "SCRUM-6589_convert_office_supplements.py"
)
_spec = importlib.util.spec_from_file_location("convert_office_supplements", _SCRIPT)
assert _spec is not None and _spec.loader is not None
script = importlib.util.module_from_spec(_spec)
cast(Any, _spec.loader).exec_module(script)

parse_years = cast(Any, script).parse_years
publication_year = cast(Any, script).publication_year
source_mod_abbreviation = cast(Any, script).source_mod_abbreviation


def test_parse_years_defaults_to_2025_and_2026():
    assert parse_years(None) == [2025, 2026]
    assert parse_years([]) == [2025, 2026]


def test_parse_years_accepts_repeats_and_commas():
    assert parse_years(["2026", "2024,2025", " 2025 "]) == [2024, 2025, 2026]


def test_parse_years_rejects_non_years():
    with pytest.raises(ValueError):
        parse_years(["25"])


def test_publication_year_prefers_date_published():
    assert publication_year("2025-03-01", "2024-12-31") == 2025
    assert publication_year("", "2024-12-31") == 2024
    assert publication_year(None, None) is None
    assert publication_year("n.d.", "soon") is None


def test_source_mod_abbreviation_first_owning_mod_or_none():
    shared = MagicMock()
    shared.mod = None
    sgd = MagicMock()
    sgd.mod = MagicMock(abbreviation="SGD")
    rf = MagicMock()
    rf.referencefile_mods = [shared, sgd]
    assert source_mod_abbreviation(rf) == "SGD"
    rf.referencefile_mods = [shared]
    assert source_mod_abbreviation(rf) is None
