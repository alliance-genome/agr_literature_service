"""Tests for Markdown schema validator."""

from agr_literature_service.lit_processing.xml2md.md_validator import (
    Severity, validate_markdown,
)
from agr_literature_service.lit_processing.xml2md.md_emitter import emit_markdown
from agr_literature_service.lit_processing.xml2md.models import (
    Author, Document, Paragraph, Reference, Section, Table, TableCell,
)


# -- Helpers ----------------------------------------------------------------

def _valid_md() -> str:
    """Return a minimal valid Markdown document."""
    return "# Title\n\n## Abstract\n\nSome text.\n\n## References\n\n1. Ref one.\n"


def _ids(result) -> list[str]:
    """Extract all rule IDs from errors and warnings."""
    return [i.rule_id for i in result.errors + result.warnings]


# -- S01: Exactly one H1 heading -------------------------------------------

class TestS01:
    def test_no_h1_is_warning(self):
        """Missing H1 is a warning (source may lack a title)."""
        result = validate_markdown("## Section\n\nText.\n")
        assert result.valid  # warnings don't make it invalid
        warn_ids = [i.rule_id for i in result.warnings]
        assert "S01" in warn_ids

    def test_multiple_h1_is_error(self):
        result = validate_markdown("# First\n\n# Second\n\nText.\n")
        assert not result.valid
        assert "S01" in [i.rule_id for i in result.errors]

    def test_single_h1_ok(self):
        result = validate_markdown(_valid_md())
        assert "S01" not in _ids(result)


# -- S02: H1 must be the first heading -------------------------------------

class TestS02:
    def test_h2_before_h1_with_h1_is_error(self):
        """H1 exists but isn't the first heading — structural error."""
        md = "## Before\n\n# Title\n\nText.\n"
        result = validate_markdown(md)
        assert not result.valid
        assert "S02" in [i.rule_id for i in result.errors]

    def test_no_h1_at_all_is_warning(self):
        """No H1 anywhere — source-data issue, warn only."""
        md = "## Section\n\nText.\n"
        result = validate_markdown(md)
        assert result.valid  # warnings don't invalidate
        assert "S02" in [i.rule_id for i in result.warnings]

    def test_h1_first_ok(self):
        result = validate_markdown(_valid_md())
        assert "S02" not in _ids(result)


# -- S03: No heading level skips -------------------------------------------

class TestS03:
    def test_skip_h2_to_h4(self):
        md = "# Title\n\n## Section\n\n#### Deep\n\nText.\n"
        result = validate_markdown(md)
        assert "S03" in _ids(result)

    def test_no_skip_ok(self):
        md = "# Title\n\n## Section\n\n### Sub\n\nText.\n"
        result = validate_markdown(md)
        assert "S03" not in _ids(result)


# -- S04: Abstract before body sections ------------------------------------

class TestS04:
    def test_abstract_not_first_h2(self):
        md = "# Title\n\n## Intro\n\nText.\n\n## Abstract\n\nText.\n"
        result = validate_markdown(md)
        assert "S04" in _ids(result)

    def test_abstract_first_ok(self):
        result = validate_markdown(_valid_md())
        assert "S04" not in _ids(result)

    def test_no_abstract_ok(self):
        md = "# Title\n\n## Intro\n\nText.\n"
        result = validate_markdown(md)
        assert "S04" not in _ids(result)


# -- S05: References is the last H2 ----------------------------------------

class TestS05:
    def test_references_not_last(self):
        md = "# Title\n\n## References\n\n1. Ref.\n\n## Appendix\n\nText.\n"
        result = validate_markdown(md)
        assert "S05" in _ids(result)

    def test_references_last_ok(self):
        result = validate_markdown(_valid_md())
        assert "S05" not in _ids(result)


# -- S06: Acknowledgments before References --------------------------------

class TestS06:
    def test_ack_after_refs(self):
        md = (
            "# Title\n\n## References\n\n1. Ref.\n\n"
            "## Acknowledgments\n\nThanks.\n"
        )
        result = validate_markdown(md)
        assert "S06" in _ids(result)

    def test_ack_before_refs_ok(self):
        md = (
            "# Title\n\n## Acknowledgments\n\nThanks.\n\n"
            "## References\n\n1. Ref.\n"
        )
        result = validate_markdown(md)
        assert "S06" not in _ids(result)


# -- S07: GFM table structure ----------------------------------------------

class TestS07:
    def test_table_without_separator_is_error(self):
        """Table with no |---| separator at all is an error."""
        md = "# Title\n\n| A | B |\n| 1 | 2 |\n"
        result = validate_markdown(md)
        assert not result.valid
        assert "S07" in [i.rule_id for i in result.errors]

    def test_multirow_header_is_warning(self):
        """Multiple header rows before separator — source data, warn only."""
        md = "# Title\n\n| A | B |\n| C | D |\n|---|---|\n| 1 | 2 |\n"
        result = validate_markdown(md)
        assert result.valid  # warnings don't invalidate
        assert "S07" in [i.rule_id for i in result.warnings]

    def test_valid_table_ok(self):
        md = "# Title\n\n| A | B |\n|---|---|\n| 1 | 2 |\n"
        result = validate_markdown(md)
        assert "S07" not in _ids(result)


# -- S08: Block elements followed by blank lines ---------------------------

class TestS08:
    def test_heading_without_blank_line(self):
        md = "# Title\n## Section\nText.\n"
        result = validate_markdown(md)
        assert "S08" in _ids(result)

    def test_heading_with_blank_line_ok(self):
        result = validate_markdown(_valid_md())
        assert "S08" not in _ids(result)


# -- S09: Trailing newline -------------------------------------------------

class TestS09:
    def test_no_trailing_newline(self):
        result = validate_markdown("# Title\n\nText.")
        assert "S09" in _ids(result)

    def test_multiple_trailing_newlines(self):
        result = validate_markdown("# Title\n\nText.\n\n")
        assert "S09" in _ids(result)

    def test_single_trailing_newline_ok(self):
        result = validate_markdown("# Title\n\nText.\n")
        assert "S09" not in _ids(result)

    def test_empty_document(self):
        result = validate_markdown("")
        assert "S09" in _ids(result)


# -- Round-trip test --------------------------------------------------------

class TestRoundTrip:
    def test_emitter_output_validates(self):
        """Markdown produced by emit_markdown() passes validation."""
        doc = Document(
            title="A Study of Gene Expression",
            authors=[
                Author(given_name="Alice", surname="Smith",
                       affiliations=["MIT"]),
            ],
            abstract=[Paragraph(text="This study examines expression.")],
            keywords=["gene expression", "RNA-seq"],
            sections=[
                Section(heading="Introduction", level=1,
                        paragraphs=[Paragraph(text="Intro text.")]),
                Section(heading="Methods", level=1,
                        paragraphs=[Paragraph(text="Methods text.")],
                        subsections=[
                            Section(heading="Samples", level=2,
                                    paragraphs=[Paragraph(text="Sample info.")]),
                        ]),
                Section(heading="Results", level=1,
                        tables=[
                            Table(label="Table 1", caption="Summary.", rows=[
                                [TableCell(text="Gene"),
                                 TableCell(text="Value")],
                                [TableCell(text="BRCA1"),
                                 TableCell(text="2.5")],
                            ]),
                        ]),
            ],
            acknowledgments="We thank the NIH.",
            references=[
                Reference(index=1, authors=["Lee C"],
                          title="Ref title", journal="Nature",
                          volume="1", year="2020"),
            ],
        )
        md = emit_markdown(doc)
        result = validate_markdown(md)
        assert result.valid, (
            f"emit_markdown output failed validation: "
            f"errors={[e.message for e in result.errors]}"
        )
        # Warnings are acceptable but there should be no errors
        assert len(result.errors) == 0


# -- Aggregate result properties -------------------------------------------

class TestValidationResult:
    def test_valid_document(self):
        result = validate_markdown(_valid_md())
        assert result.valid is True
        assert len(result.errors) == 0

    def test_severity_classification(self):
        """Errors go to errors list, warnings to warnings list."""
        # Multiple H1 triggers an error; missing blank line triggers a warning
        md = "# First\n# Second\nText.\n"
        result = validate_markdown(md)
        assert len(result.errors) > 0
        assert len(result.warnings) > 0
        for issue in result.errors:
            assert issue.severity == Severity.ERROR
        for issue in result.warnings:
            assert issue.severity == Severity.WARNING
