"""Tests for the converter orchestrator."""

import gzip
from pathlib import Path

import pytest

from agr_literature_service.lit_processing.xml2md.converter import (
    convert_xml_to_markdown, detect_format,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"
_HAS_FIXTURES = FIXTURES_DIR.is_dir() and any(FIXTURES_DIR.glob("*.gz"))


# Minimal TEI fixture for format detection / conversion
MINIMAL_TEI = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<TEI xml:space="preserve" xmlns="http://www.tei-c.org/ns/1.0">
  <teiHeader xml:lang="en">
    <fileDesc>
      <titleStmt><title level="a" type="main">TEI Title</title></titleStmt>
      <publicationStmt><publisher/></publicationStmt>
      <sourceDesc>
        <biblStruct>
          <analytic><title level="a" type="main">TEI Title</title></analytic>
          <monogr><imprint><date when="2024"/></imprint></monogr>
        </biblStruct>
      </sourceDesc>
    </fileDesc>
    <profileDesc/>
  </teiHeader>
  <text xml:lang="en">
    <body>
      <div xmlns="http://www.tei-c.org/ns/1.0">
        <head>Introduction</head>
        <p>TEI body text.</p>
      </div>
    </body>
    <back><div type="references"><listBibl/></div></back>
  </text>
</TEI>
"""

# Minimal JATS fixture for format detection / conversion
MINIMAL_JATS = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<article article-type="research-article">
  <front>
    <article-meta>
      <title-group><article-title>JATS Title</article-title></title-group>
      <abstract><p>JATS abstract.</p></abstract>
    </article-meta>
  </front>
  <body>
    <sec><title>Introduction</title><p>JATS body text.</p></sec>
  </body>
  <back><ref-list/></back>
</article>
"""


class TestDetectFormat:
    """Tests for detect_format function."""

    def test_detect_tei_format(self):
        """TEI namespace detected -> 'tei'."""
        fmt = detect_format(MINIMAL_TEI)
        assert fmt == "tei"

    def test_detect_jats_format(self):
        """<article> root element detected -> 'jats'."""
        fmt = detect_format(MINIMAL_JATS)
        assert fmt == "jats"

    def test_detect_unknown_format(self):
        """Non-XML or unknown root raises ValueError."""
        with pytest.raises(ValueError, match="Unknown"):
            detect_format(b"<html><body>Not XML paper</body></html>")

    def test_detect_invalid_xml(self):
        """Invalid XML raises ValueError."""
        with pytest.raises(ValueError):
            detect_format(b"this is not xml at all")


class TestConverter:
    """Tests for convert_xml_to_markdown function."""

    def test_convert_tei_auto(self):
        """Auto-detect TEI and convert to markdown."""
        md = convert_xml_to_markdown(MINIMAL_TEI)
        assert "# TEI Title" in md
        assert "Introduction" in md
        assert "TEI body text." in md

    def test_convert_jats_auto(self):
        """Auto-detect JATS and convert to markdown."""
        md = convert_xml_to_markdown(MINIMAL_JATS)
        assert "# JATS Title" in md
        assert "Introduction" in md
        assert "JATS body text." in md

    def test_convert_explicit_tei(self):
        """Explicit source_format='tei' bypasses detection."""
        md = convert_xml_to_markdown(MINIMAL_TEI, source_format="tei")
        assert "# TEI Title" in md

    def test_convert_explicit_jats(self):
        """Explicit source_format='jats' bypasses detection."""
        md = convert_xml_to_markdown(MINIMAL_JATS, source_format="jats")
        assert "# JATS Title" in md

    def test_convert_unknown_format_raises(self):
        """source_format='foo' raises ValueError."""
        with pytest.raises(ValueError, match="Unknown format"):
            convert_xml_to_markdown(MINIMAL_TEI, source_format="foo")

    def test_convert_explicit_format_with_invalid_xml(self):
        """Explicit format with garbage input gives ValueError, not AttributeError."""
        with pytest.raises(ValueError):
            convert_xml_to_markdown(b"not xml at all", source_format="tei")

    def test_convert_explicit_format_with_garbage_jats(self):
        """Explicit JATS format with garbage input gives ValueError."""
        with pytest.raises(ValueError):
            convert_xml_to_markdown(b"totally invalid", source_format="jats")


def _load_fixture(filename: str) -> bytes:
    """Load a gzipped TEI fixture file."""
    filepath = FIXTURES_DIR / filename
    with gzip.open(filepath, "rb") as f:
        return f.read()


@pytest.mark.skipif(not _HAS_FIXTURES, reason="No .gz fixtures in fixtures/")
class TestRealTeiConversion:
    """Integration tests using real TEI files from production S3."""

    def test_convert_real_tei_with_figures(self):
        """Full conversion of real paper with figures, keywords, footnotes."""
        xml = _load_fixture("tei_with_figures_keywords.tei.gz")
        md = convert_xml_to_markdown(xml)
        assert isinstance(md, str)
        assert len(md) > 1000
        # Title
        assert "Isoscoparin" in md
        # Has sections
        assert "##" in md
        # Has figures
        assert "**Figure" in md or "**Fig" in md
        # Has keywords
        assert "**Keywords:**" in md
        # Has references
        assert "## References" in md

    def test_convert_real_tei_with_tables(self):
        """Full conversion of real paper with tables."""
        xml = _load_fixture("tei_with_tables.tei.gz")
        md = convert_xml_to_markdown(xml)
        assert isinstance(md, str)
        assert len(md) > 1000
        # Title
        assert "morphogenesis" in md.lower() or "microvilliated" in md.lower()
        # Has table markup
        assert "|" in md
        assert "|---|" in md
        # Has keywords
        assert "**Keywords:**" in md
        # Has references
        assert "## References" in md

    def test_convert_real_tei_no_abstract(self):
        """Full conversion of real paper with no abstract, no DOI."""
        xml = _load_fixture("tei_no_abstract_no_doi.tei.gz")
        md = convert_xml_to_markdown(xml)
        assert isinstance(md, str)
        assert len(md) > 500
        # Title
        assert "NTF2" in md
        # No abstract section
        assert "## Abstract" not in md
        # Has body sections
        assert "##" in md
        # Has references
        assert "## References" in md
