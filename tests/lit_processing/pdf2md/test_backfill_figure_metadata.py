"""Unit tests for the figure-metadata backfill planning logic (SCRUM-6246).

Only the pure, side-effect-free helpers are unit-tested here; the DB/PDFX
orchestration (select_references_needing_metadata, backfill_reference,
_regenerate_image_manifest_marker_only) is integration-level and marked
``# pragma: no cover``.
"""
from types import SimpleNamespace

from agr_literature_service.lit_processing.pdf2md.backfill_figure_metadata import (
    _first_mod_abbreviation,
    plan_sidecars_for_source,
)


def _png(display_name):
    return SimpleNamespace(display_name=display_name)


class TestPlanSidecarsForSource:
    def test_pairs_manifest_to_sorted_pngs(self):
        # PNG rows provided out of order; planning sorts by display_name.
        png_rows = [_png("paper_image_002"), _png("paper_image_001")]
        images = [{"caption_text": "first"}, {"caption_text": "second"}]
        planned, skipped, skip_reason = plan_sidecars_for_source(
            png_rows, images, existing_metadata_names=set()
        )
        assert skip_reason is None
        assert skipped == 0
        assert [p["figure_display_name"] for p in planned] == [
            "paper_image_001", "paper_image_002",
        ]
        assert [p["figure_index"] for p in planned] == [1, 2]
        # Manifest entries are paired in order with the sorted PNGs.
        assert planned[0]["image"] == {"caption_text": "first"}
        assert planned[1]["image"] == {"caption_text": "second"}

    def test_figure_index_parsed_from_display_name_not_position(self):
        """figure_index comes from the PNG's ``_image_NNN`` ordinal, so a
        gap (e.g. image 002 failed to download originally) yields [1, 3], not
        the positional [1, 2] — keeping it consistent with the live path."""
        png_rows = [_png("paper_image_001"), _png("paper_image_003")]
        images = [{"a": 1}, {"a": 2}]
        planned, _skipped, skip_reason = plan_sidecars_for_source(
            png_rows, images, existing_metadata_names=set()
        )
        assert skip_reason is None
        assert [p["figure_index"] for p in planned] == [1, 3]

    def test_skips_pngs_that_already_have_a_sidecar(self):
        png_rows = [_png("paper_image_001"), _png("paper_image_002")]
        images = [{"a": 1}, {"a": 2}]
        planned, skipped, skip_reason = plan_sidecars_for_source(
            png_rows, images, existing_metadata_names={"paper_image_001"}
        )
        assert skip_reason is None
        assert skipped == 1
        assert [p["figure_display_name"] for p in planned] == ["paper_image_002"]
        # figure_index reflects the figure's own ordinal (parsed from the
        # display_name), not its position among the still-missing subset.
        assert planned[0]["figure_index"] == 2

    def test_all_sidecars_present_plans_nothing(self):
        png_rows = [_png("paper_image_001"), _png("paper_image_002")]
        images = [{"a": 1}, {"a": 2}]
        planned, skipped, skip_reason = plan_sidecars_for_source(
            png_rows, images,
            existing_metadata_names={"paper_image_001", "paper_image_002"},
        )
        assert planned == []
        assert skipped == 2
        assert skip_reason is None

    def test_count_mismatch_skips_source(self):
        png_rows = [_png("paper_image_001"), _png("paper_image_002")]
        images = [{"a": 1}, {"a": 2}, {"a": 3}]  # one extra image
        planned, skipped, skip_reason = plan_sidecars_for_source(
            png_rows, images, existing_metadata_names=set()
        )
        assert skip_reason is not None
        assert "manifest has 3 image(s) but 2 existing" in skip_reason
        assert planned == []
        assert skipped == 0

    def test_fewer_images_than_figures_skips_source(self):
        png_rows = [_png("paper_image_001"), _png("paper_image_002")]
        images = [{"a": 1}]
        _planned, _skipped, skip_reason = plan_sidecars_for_source(
            png_rows, images, existing_metadata_names=set()
        )
        assert skip_reason is not None

    def test_non_canonical_png_name_skips_source(self):
        """A renamed PNG (``_image_002_1`` from a prior re-conversion) breaks
        the sorted-order==manifest-order assumption, so the whole source is
        skipped rather than risk attaching a caption to the wrong figure —
        even though the image count happens to match."""
        png_rows = [_png("paper_image_001"), _png("paper_image_002_1")]
        images = [{"a": 1}, {"a": 2}]
        planned, _skipped, skip_reason = plan_sidecars_for_source(
            png_rows, images, existing_metadata_names=set()
        )
        assert skip_reason is not None
        assert "non-canonical" in skip_reason
        assert planned == []


class TestFirstModAbbreviation:
    def test_returns_first_non_null_mod(self):
        ref_file = SimpleNamespace(referencefile_mods=[
            SimpleNamespace(mod=None),
            SimpleNamespace(mod=SimpleNamespace(abbreviation="WB")),
            SimpleNamespace(mod=SimpleNamespace(abbreviation="ZFIN")),
        ])
        assert _first_mod_abbreviation(ref_file) == "WB"

    def test_returns_none_when_only_null_mod(self):
        ref_file = SimpleNamespace(referencefile_mods=[SimpleNamespace(mod=None)])
        assert _first_mod_abbreviation(ref_file) is None

    def test_returns_none_when_no_associations(self):
        assert _first_mod_abbreviation(SimpleNamespace(referencefile_mods=[])) is None
        assert _first_mod_abbreviation(SimpleNamespace(referencefile_mods=None)) is None
