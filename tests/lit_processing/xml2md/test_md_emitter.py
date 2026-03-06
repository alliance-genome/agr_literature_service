"""Tests for Markdown emitter."""

from agr_literature_service.lit_processing.xml2md.models import (
    Author, Document, Figure, Formula, ListBlock,
    Paragraph, Reference, Section, Table, TableCell,
)
from agr_literature_service.lit_processing.xml2md.md_emitter import emit_markdown


def _make_doc(**kwargs) -> Document:
    """Helper to create a Document with defaults overridden by kwargs."""
    return Document(**kwargs)


class TestMdEmitter:
    """Tests for emit_markdown function."""

    def test_emit_title(self):
        """Title becomes '# Title'."""
        md = emit_markdown(_make_doc(title="My Paper Title"))
        assert "# My Paper Title" in md

    def test_emit_authors(self):
        """Authors as plain comma-separated names."""
        doc = _make_doc(authors=[
            Author(given_name="Alice", surname="Smith",
                   affiliations=["Dept of Biology, MIT"]),
            Author(given_name="Bob", surname="Jones",
                   affiliations=["Dept of CS, Stanford"]),
        ])
        md = emit_markdown(doc)
        assert "Alice Smith, Bob Jones" in md
        # No 'Authors:' prefix (consensus pipeline format)
        assert "Authors:" not in md

    def test_emit_abstract(self):
        """Abstract section: '## Abstract' + paragraphs."""
        doc = _make_doc(abstract=[
            Paragraph(text="First abstract paragraph."),
            Paragraph(text="Second abstract paragraph."),
        ])
        md = emit_markdown(doc)
        assert "## Abstract" in md
        assert "First abstract paragraph." in md
        assert "Second abstract paragraph." in md

    def test_emit_keywords(self):
        """Keywords as bold inline list."""
        doc = _make_doc(keywords=["gene expression", "RNA-seq", "transcriptomics"])
        md = emit_markdown(doc)
        assert "**Keywords:**" in md
        assert "gene expression" in md
        assert "RNA-seq" in md

    def test_emit_sections_with_numbers(self):
        """Section numbers omitted from headings (consensus format)."""
        doc = _make_doc(sections=[
            Section(heading="Introduction", number="1", level=1,
                    paragraphs=[Paragraph(text="Intro text.")]),
        ])
        md = emit_markdown(doc)
        assert "## Introduction" in md
        assert "## 1 " not in md
        assert "Intro text." in md

    def test_emit_sections_without_numbers(self):
        """'## Introduction' when no number."""
        doc = _make_doc(sections=[
            Section(heading="Introduction", number="", level=1,
                    paragraphs=[Paragraph(text="Intro text.")]),
        ])
        md = emit_markdown(doc)
        assert "## Introduction" in md

    def test_emit_nested_sections(self):
        """Subsections use ### and #### headings."""
        doc = _make_doc(sections=[
            Section(heading="Methods", number="2", level=1,
                    subsections=[
                        Section(heading="Samples", number="2.1", level=2,
                                paragraphs=[Paragraph(text="Sample text.")],
                                subsections=[
                                    Section(heading="RNA extraction", number="2.1.1",
                                            level=3,
                                            paragraphs=[Paragraph(text="RNA text.")])
                                ]),
                    ]),
        ])
        md = emit_markdown(doc)
        assert "## Methods" in md
        assert "### Samples" in md
        assert "#### RNA extraction" in md

    def test_emit_heading_level_cap(self):
        """Max heading level is ######."""
        doc = _make_doc(sections=[
            Section(heading="Deep", level=1, subsections=[
                Section(heading="Deeper", level=2, subsections=[
                    Section(heading="Deepest", level=3, subsections=[
                        Section(heading="VeryDeep", level=4, subsections=[
                            Section(heading="UltraDeep", level=5, subsections=[
                                Section(heading="MaxDeep", level=6,
                                        paragraphs=[Paragraph(text="Bottom.")]),
                            ]),
                        ]),
                    ]),
                ]),
            ]),
        ])
        md = emit_markdown(doc)
        # Level 6 -> ####### (7 hashes) but capped at ###### (6)
        assert "###### MaxDeep" in md
        # Ensure no 7-hash heading
        assert "####### " not in md

    def test_emit_figures(self):
        """'**Figure 1.** Caption text'."""
        doc = _make_doc(sections=[
            Section(heading="Results", level=1, figures=[
                Figure(label="Figure 1.", caption="Expression levels across conditions."),
            ]),
        ])
        md = emit_markdown(doc)
        assert "**Figure 1.**" in md
        assert "Expression levels across conditions." in md

    def test_emit_tables_gfm(self):
        """GFM table with | header | and |---| separator."""
        doc = _make_doc(sections=[
            Section(heading="Results", level=1, tables=[
                Table(label="Table 1", caption="Summary.", rows=[
                    [TableCell(text="Gene"), TableCell(text="Expression")],
                    [TableCell(text="BRCA1"), TableCell(text="2.5")],
                ]),
            ]),
        ])
        md = emit_markdown(doc)
        assert "| Gene | Expression |" in md
        assert "|---|---|" in md
        assert "| BRCA1 | 2.5 |" in md

    def test_emit_table_with_caption(self):
        """Table followed by bold caption."""
        doc = _make_doc(sections=[
            Section(heading="Results", level=1, tables=[
                Table(label="Table 1", caption="Summary of findings.", rows=[
                    [TableCell(text="A"), TableCell(text="B")],
                    [TableCell(text="1"), TableCell(text="2")],
                ]),
            ]),
        ])
        md = emit_markdown(doc)
        assert "**Table 1.** Summary of findings." in md

    def test_emit_formulas(self):
        """Formulas emitted as plain text."""
        doc = _make_doc(sections=[
            Section(heading="Theory", level=1, formulas=[
                Formula(text="E = mc^2", label="(1)"),
                Formula(text="F = ma", label=""),
            ]),
        ])
        md = emit_markdown(doc)
        assert "E = mc^2" in md
        assert "F = ma" in md
        # No blockquote format (consensus pipeline format)
        assert "> Formula" not in md

    def test_emit_lists_unordered(self):
        """'- item' format."""
        doc = _make_doc(sections=[
            Section(heading="Methods", level=1, lists=[
                ListBlock(items=["Step one", "Step two", "Step three"],
                          ordered=False),
            ]),
        ])
        md = emit_markdown(doc)
        assert "- Step one" in md
        assert "- Step two" in md
        assert "- Step three" in md

    def test_emit_lists_ordered(self):
        """'1. item' format."""
        doc = _make_doc(sections=[
            Section(heading="Methods", level=1, lists=[
                ListBlock(items=["First", "Second", "Third"], ordered=True),
            ]),
        ])
        md = emit_markdown(doc)
        assert "1. First" in md
        assert "2. Second" in md
        assert "3. Third" in md

    def test_emit_footnotes(self):
        """'[^n]: text' format."""
        doc = _make_doc(sections=[
            Section(heading="Discussion", level=1, notes=[
                "Additional methodological details.",
                "See supplementary materials.",
            ]),
        ])
        md = emit_markdown(doc)
        assert "[^1]: Additional methodological details." in md
        assert "[^2]: See supplementary materials." in md

    def test_emit_acknowledgments(self):
        """'## Acknowledgments' section."""
        doc = _make_doc(acknowledgments="We thank the NIH for funding.")
        md = emit_markdown(doc)
        assert "## Acknowledgments" in md
        assert "We thank the NIH for funding." in md

    def test_emit_references(self):
        """Numbered reference list: '1. Author A, Author B (2024)...'."""
        doc = _make_doc(references=[
            Reference(index=1, authors=["Lee C", "Park D"],
                      title="Genomic analysis of expression",
                      journal="Nature Genetics", volume="52", issue="3",
                      pages="100-110", year="2020", doi="10.1038/ng.2020"),
            Reference(index=2, authors=["Wang E"],
                      title="RNA-seq best practices",
                      journal="Bioinformatics", volume="36",
                      pages="200-215", year="2019"),
        ])
        md = emit_markdown(doc)
        assert "## References" in md
        assert "1. Lee C, Park D" in md
        assert "(2020)" in md
        assert "Genomic analysis of expression" in md
        assert "*Nature Genetics*" in md
        assert "52" in md
        assert "100-110" in md
        assert "doi:10.1038/ng.2020" in md
        assert "2. Wang E" in md

    def test_emit_empty_document(self):
        """Empty document produces minimal valid markdown."""
        md = emit_markdown(Document())
        # Should not crash, should produce some output
        assert isinstance(md, str)
        # No sections means no headings beyond possibly empty title
        assert "## Abstract" not in md
        assert "## References" not in md

    def test_emit_full_document(self):
        """End-to-end: complete Document -> expected markdown string."""
        doc = Document(
            title="A Study of Gene Expression",
            authors=[
                Author(given_name="Alice", surname="Smith",
                       affiliations=["Dept of Biology, MIT"]),
                Author(given_name="Bob", surname="Jones",
                       affiliations=["Dept of CS, Stanford"]),
            ],
            abstract=[Paragraph(text="This study examines gene expression.")],
            keywords=["gene expression", "RNA-seq"],
            doi="10.1234/example.2024",
            sections=[
                Section(heading="Introduction", number="1", level=1,
                        paragraphs=[Paragraph(text="Intro text here.")]),
                Section(heading="Methods", number="2", level=1,
                        paragraphs=[Paragraph(text="Methods text here.")]),
            ],
            acknowledgments="We thank the NIH.",
            references=[
                Reference(index=1, authors=["Lee C"],
                          title="Ref title", journal="Nature",
                          volume="1", year="2020"),
            ],
            source_format="tei",
        )
        md = emit_markdown(doc)
        assert md.startswith("# A Study of Gene Expression")
        assert "Alice Smith, Bob Jones" in md
        assert "## Abstract" in md
        assert "**Keywords:**" in md
        assert "## Introduction" in md
        assert "## Methods" in md
        assert "## Acknowledgments" in md
        assert "## References" in md
        assert "1. Lee C (2020)" in md

    def test_emit_reference_with_editors_and_publisher(self):
        """Editors, chapter title, publisher, conference emitted."""
        doc = _make_doc(
            title="T",
            references=[
                Reference(
                    index=1, authors=["Auth A"],
                    title="Book chapter title",
                    chapter_title="Part One",
                    editors=["Editor E"],
                    journal="Big Book",
                    publisher="Academic Press",
                    publisher_loc="New York",
                    year="2023",
                ),
                Reference(
                    index=2, authors=["Auth B"],
                    title="Conference paper",
                    conference="ISMB 2024",
                    year="2024",
                ),
            ],
        )
        md = emit_markdown(doc)
        assert "In: Part One." in md
        assert "Edited by Editor E." in md
        assert "New York: Academic Press." in md
        assert "*ISMB 2024*." in md

    def test_emit_table_footnotes(self):
        """Table footnotes emitted after caption."""
        doc = _make_doc(sections=[
            Section(heading="Results", level=1, tables=[
                Table(label="Table 1", caption="Summary.", rows=[
                    [TableCell(text="A"), TableCell(text="B")],
                    [TableCell(text="1"), TableCell(text="2")],
                ], foot_notes=["FC, fold change.", "*P < 0.05."]),
            ]),
        ])
        md = emit_markdown(doc)
        assert "FC, fold change." in md
        assert "*P < 0.05." in md

    def test_emit_reference_with_pmcid_and_ext_links(self):
        """PMCID and ext_links emitted in references."""
        doc = _make_doc(
            title="T",
            references=[
                Reference(
                    index=1, authors=["Doe J"],
                    title="Test paper", journal="PLOS ONE",
                    year="2024", doi="10.1234/test",
                    pmid="11111111", pmcid="PMC9999999",
                    ext_links=["https://example.com/data"],
                ),
            ],
        )
        md = emit_markdown(doc)
        assert "doi:10.1234/test" in md
        assert "PMID:11111111" in md
        assert "PMCID:PMC9999999" in md
        assert "https://example.com/data" in md
