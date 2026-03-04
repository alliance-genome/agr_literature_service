"""Tests for TEI XML parser."""

from agr_literature_service.lit_processing.xml2md.tei_parser import parse_tei


# -- TEI XML test fixtures --------------------------------------------------

FULL_TEI = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<TEI xml:space="preserve" xmlns="http://www.tei-c.org/ns/1.0">
  <teiHeader xml:lang="en">
    <fileDesc>
      <titleStmt>
        <title level="a" type="main">A Study of Gene Expression</title>
      </titleStmt>
      <publicationStmt><publisher/></publicationStmt>
      <sourceDesc>
        <biblStruct>
          <analytic>
            <author>
              <persName><forename type="first">Alice</forename><surname>Smith</surname></persName>
              <email>alice@example.com</email>
              <affiliation>
                <orgName type="department">Dept of Biology</orgName>
                <orgName type="institution">MIT</orgName>
              </affiliation>
            </author>
            <author>
              <persName><forename type="first">Bob</forename><surname>Jones</surname></persName>
              <affiliation>
                <orgName type="department">Dept of CS</orgName>
                <orgName type="institution">Stanford</orgName>
              </affiliation>
            </author>
            <title level="a" type="main">A Study of Gene Expression</title>
          </analytic>
          <monogr><imprint><date when="2024"/></imprint></monogr>
          <idno type="DOI">10.1234/example.2024</idno>
        </biblStruct>
      </sourceDesc>
    </fileDesc>
    <profileDesc>
      <textClass>
        <keywords>
          <term>gene expression</term>
          <term>RNA-seq</term>
          <term>transcriptomics</term>
        </keywords>
      </textClass>
      <abstract>
        <div xmlns="http://www.tei-c.org/ns/1.0">
          <p>This study examines gene expression patterns.</p>
          <p>We used RNA-seq to analyze samples.</p>
        </div>
      </abstract>
    </profileDesc>
  </teiHeader>
  <text xml:lang="en">
    <body>
      <div xmlns="http://www.tei-c.org/ns/1.0">
        <head n="1">Introduction</head>
        <p>Gene expression is fundamental to <ref type="bibr" target="#b0">[1]</ref> biology.</p>
        <p>Previous work showed <ref type="bibr" target="#b1">(Jones, 2020)</ref> that...</p>
      </div>
      <div xmlns="http://www.tei-c.org/ns/1.0">
        <head n="2">Methods</head>
        <p>We performed RNA-seq analysis.</p>
        <div>
          <head n="2.1">Sample Preparation</head>
          <p>Samples were prepared using standard protocols.</p>
        </div>
        <div>
          <head n="2.2">Data Analysis</head>
          <p>Data was analyzed using custom scripts.</p>
        </div>
      </div>
      <div xmlns="http://www.tei-c.org/ns/1.0">
        <head n="3">Results</head>
        <p>We found significant differences.</p>
        <figure xml:id="fig_0">
          <head>Figure 1.</head>
          <label>1</label>
          <figDesc>Expression levels across conditions.</figDesc>
          <graphic url="fig1.png"/>
        </figure>
      </div>
      <div xmlns="http://www.tei-c.org/ns/1.0">
        <head n="4">Discussion</head>
        <p>Our results align with previous findings.</p>
        <note place="foot" n="1">Additional methodological details available.</note>
      </div>
    </body>
    <back>
      <div type="acknowledgement">
        <div><head>Acknowledgements</head>
        <p>We thank the NIH for funding.</p></div>
      </div>
      <div type="annex">
        <div><head>Author Contributions</head>
        <p>AS designed the study. BJ performed the analysis.</p></div>
      </div>
      <div type="references">
        <listBibl>
          <biblStruct xml:id="b0">
            <analytic>
              <title level="a">Genomic analysis of expression</title>
              <author><persName><forename>C</forename><surname>Lee</surname></persName></author>
              <author><persName><forename>D</forename><surname>Park</surname></persName></author>
            </analytic>
            <monogr>
              <title level="j">Nature Genetics</title>
              <imprint>
                <biblScope unit="volume">52</biblScope>
                <biblScope unit="issue">3</biblScope>
                <biblScope unit="page" from="100" to="110"/>
                <date type="published" when="2020"/>
              </imprint>
            </monogr>
            <idno type="DOI">10.1038/ng.2020</idno>
          </biblStruct>
          <biblStruct xml:id="b1">
            <analytic>
              <title level="a">RNA-seq best practices</title>
              <author><persName><forename>E</forename><surname>Wang</surname></persName></author>
            </analytic>
            <monogr>
              <title level="j">Bioinformatics</title>
              <imprint>
                <biblScope unit="volume">36</biblScope>
                <biblScope unit="page" from="200" to="215"/>
                <date type="published" when="2019"/>
              </imprint>
            </monogr>
          </biblStruct>
        </listBibl>
      </div>
    </back>
  </text>
</TEI>
"""

NO_ABSTRACT_TEI = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<TEI xml:space="preserve" xmlns="http://www.tei-c.org/ns/1.0">
  <teiHeader xml:lang="en">
    <fileDesc>
      <titleStmt>
        <title level="a" type="main">Paper Without Abstract</title>
      </titleStmt>
      <publicationStmt><publisher/></publicationStmt>
      <sourceDesc>
        <biblStruct>
          <analytic>
            <author>
              <persName><forename type="first">Jane</forename><surname>Doe</surname></persName>
            </author>
            <title level="a" type="main">Paper Without Abstract</title>
          </analytic>
          <monogr><imprint><date when="2023"/></imprint></monogr>
        </biblStruct>
      </sourceDesc>
    </fileDesc>
    <profileDesc/>
  </teiHeader>
  <text xml:lang="en">
    <body>
      <div xmlns="http://www.tei-c.org/ns/1.0">
        <head>Introduction</head>
        <p>Some introductory text.</p>
      </div>
    </body>
    <back>
      <div type="references"><listBibl/></div>
    </back>
  </text>
</TEI>
"""

TABLE_TEI = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<TEI xml:space="preserve" xmlns="http://www.tei-c.org/ns/1.0">
  <teiHeader xml:lang="en">
    <fileDesc>
      <titleStmt>
        <title level="a" type="main">Paper With Tables</title>
      </titleStmt>
      <publicationStmt><publisher/></publicationStmt>
      <sourceDesc>
        <biblStruct>
          <analytic>
            <title level="a" type="main">Paper With Tables</title>
          </analytic>
          <monogr><imprint><date when="2024"/></imprint></monogr>
        </biblStruct>
      </sourceDesc>
    </fileDesc>
    <profileDesc/>
  </teiHeader>
  <text xml:lang="en">
    <body>
      <div xmlns="http://www.tei-c.org/ns/1.0">
        <head n="1">Results</head>
        <p>Table 1 shows our results.</p>
        <figure type="table" xml:id="tab_0">
          <head>Table 1</head>
          <label>Table 1</label>
          <figDesc>Summary of findings.</figDesc>
          <table>
            <row><cell>Gene</cell><cell>Expression</cell><cell>P-value</cell></row>
            <row><cell>BRCA1</cell><cell>2.5</cell><cell>0.001</cell></row>
            <row><cell>TP53</cell><cell>1.8</cell><cell>0.05</cell></row>
          </table>
        </figure>
      </div>
    </body>
    <back>
      <div type="references"><listBibl/></div>
    </back>
  </text>
</TEI>
"""

EMPTY_HEADING_TEI = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<TEI xml:space="preserve" xmlns="http://www.tei-c.org/ns/1.0">
  <teiHeader xml:lang="en">
    <fileDesc>
      <titleStmt>
        <title level="a" type="main">Paper With Empty Headings</title>
      </titleStmt>
      <publicationStmt><publisher/></publicationStmt>
      <sourceDesc>
        <biblStruct>
          <analytic>
            <title level="a" type="main">Paper With Empty Headings</title>
          </analytic>
          <monogr><imprint><date when="2024"/></imprint></monogr>
        </biblStruct>
      </sourceDesc>
    </fileDesc>
    <profileDesc/>
  </teiHeader>
  <text xml:lang="en">
    <body>
      <div xmlns="http://www.tei-c.org/ns/1.0">
        <head/>
        <p>Content under empty heading.</p>
      </div>
    </body>
    <back>
      <div type="references"><listBibl/></div>
    </back>
  </text>
</TEI>
"""

HEADLESS_DIV_TEI = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<TEI xml:space="preserve" xmlns="http://www.tei-c.org/ns/1.0">
  <teiHeader xml:lang="en">
    <fileDesc>
      <titleStmt>
        <title level="a" type="main">Paper With Headless Divs</title>
      </titleStmt>
      <publicationStmt><publisher/></publicationStmt>
      <sourceDesc>
        <biblStruct>
          <analytic>
            <title level="a" type="main">Paper With Headless Divs</title>
          </analytic>
          <monogr><imprint><date when="2024"/></imprint></monogr>
        </biblStruct>
      </sourceDesc>
    </fileDesc>
    <profileDesc/>
  </teiHeader>
  <text xml:lang="en">
    <body>
      <div xmlns="http://www.tei-c.org/ns/1.0">
        <p>Paragraph without any heading element.</p>
        <p>Another paragraph in headless div.</p>
      </div>
    </body>
    <back>
      <div type="references"><listBibl/></div>
    </back>
  </text>
</TEI>
"""

FORMULA_LIST_TEI = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<TEI xml:space="preserve" xmlns="http://www.tei-c.org/ns/1.0">
  <teiHeader xml:lang="en">
    <fileDesc>
      <titleStmt>
        <title level="a" type="main">Paper With Formulas and Lists</title>
      </titleStmt>
      <publicationStmt><publisher/></publicationStmt>
      <sourceDesc>
        <biblStruct>
          <analytic>
            <title level="a" type="main">Paper With Formulas and Lists</title>
          </analytic>
          <monogr><imprint><date when="2024"/></imprint></monogr>
        </biblStruct>
      </sourceDesc>
    </fileDesc>
    <profileDesc/>
  </teiHeader>
  <text xml:lang="en">
    <body>
      <div xmlns="http://www.tei-c.org/ns/1.0">
        <head n="1">Methods</head>
        <p>We used the following formula:</p>
        <formula xml:id="formula_0">E = mc^2 <label>(1)</label></formula>
        <p>The key steps are:</p>
        <list>
          <item>Step one: collect data</item>
          <item>Step two: analyze results</item>
          <item>Step three: validate findings</item>
        </list>
      </div>
    </body>
    <back>
      <div type="references"><listBibl/></div>
    </back>
  </text>
</TEI>
"""

DIRECT_P_ABSTRACT_TEI = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<TEI xml:space="preserve" xmlns="http://www.tei-c.org/ns/1.0">
  <teiHeader xml:lang="en">
    <fileDesc>
      <titleStmt>
        <title level="a" type="main">Paper With Direct P Abstract</title>
      </titleStmt>
      <publicationStmt><publisher/></publicationStmt>
      <sourceDesc>
        <biblStruct>
          <analytic>
            <title level="a" type="main">Paper With Direct P Abstract</title>
          </analytic>
          <monogr><imprint><date when="2024"/></imprint></monogr>
        </biblStruct>
      </sourceDesc>
    </fileDesc>
    <profileDesc>
      <abstract>
        <p>This is a direct paragraph abstract without a wrapping div.</p>
      </abstract>
    </profileDesc>
  </teiHeader>
  <text xml:lang="en">
    <body>
      <div xmlns="http://www.tei-c.org/ns/1.0">
        <head>Content</head>
        <p>Body text.</p>
      </div>
    </body>
    <back>
      <div type="references"><listBibl/></div>
    </back>
  </text>
</TEI>
"""


class TestTeiParser:
    """Tests for parse_tei function."""

    def test_parse_title(self):
        """Title extracted from //teiHeader//title[@level='a']."""
        doc = parse_tei(FULL_TEI)
        assert doc.title == "A Study of Gene Expression"

    def test_parse_authors(self):
        """Authors with forename, surname, email, affiliations."""
        doc = parse_tei(FULL_TEI)
        assert len(doc.authors) == 2
        assert doc.authors[0].given_name == "Alice"
        assert doc.authors[0].surname == "Smith"
        assert doc.authors[0].email == "alice@example.com"
        assert any("MIT" in a for a in doc.authors[0].affiliations)
        assert doc.authors[1].given_name == "Bob"
        assert doc.authors[1].surname == "Jones"

    def test_parse_abstract_div_p(self):
        """Abstract with <div><p> structure (most common)."""
        doc = parse_tei(FULL_TEI)
        assert len(doc.abstract) == 2
        assert "gene expression" in doc.abstract[0].text
        assert "RNA-seq" in doc.abstract[1].text

    def test_parse_abstract_direct_p(self):
        """Abstract with direct <p> (less common variant)."""
        doc = parse_tei(DIRECT_P_ABSTRACT_TEI)
        assert len(doc.abstract) == 1
        assert "direct paragraph abstract" in doc.abstract[0].text

    def test_parse_no_abstract(self):
        """Paper with no abstract returns empty list."""
        doc = parse_tei(NO_ABSTRACT_TEI)
        assert doc.abstract == []

    def test_parse_keywords(self):
        """Keywords from //textClass//keywords/term."""
        doc = parse_tei(FULL_TEI)
        assert len(doc.keywords) == 3
        assert "gene expression" in doc.keywords
        assert "RNA-seq" in doc.keywords
        assert "transcriptomics" in doc.keywords

    def test_parse_doi(self):
        """DOI from //sourceDesc//idno[@type='DOI']."""
        doc = parse_tei(FULL_TEI)
        assert doc.doi == "10.1234/example.2024"

    def test_parse_no_doi(self):
        """Paper with no DOI returns empty string."""
        doc = parse_tei(NO_ABSTRACT_TEI)
        assert doc.doi == ""

    def test_parse_body_sections(self):
        """Body divs become sections with headings and paragraphs."""
        doc = parse_tei(FULL_TEI)
        assert len(doc.sections) == 4
        assert doc.sections[0].heading == "Introduction"
        assert doc.sections[1].heading == "Methods"
        assert doc.sections[2].heading == "Results"
        assert doc.sections[3].heading == "Discussion"

    def test_parse_section_numbering(self):
        """Section numbers from <head n='1'>."""
        doc = parse_tei(FULL_TEI)
        assert doc.sections[0].number == "1"
        assert doc.sections[1].number == "2"
        assert doc.sections[2].number == "3"
        assert doc.sections[3].number == "4"

    def test_parse_nested_sections(self):
        """Nested divs produce subsections with correct levels."""
        doc = parse_tei(FULL_TEI)
        methods = doc.sections[1]
        assert len(methods.subsections) == 2
        assert methods.subsections[0].heading == "Sample Preparation"
        assert methods.subsections[0].number == "2.1"
        assert methods.subsections[0].level == 2
        assert methods.subsections[1].heading == "Data Analysis"
        assert methods.subsections[1].number == "2.2"

    def test_parse_empty_heading(self):
        """Empty <head> element handled gracefully."""
        doc = parse_tei(EMPTY_HEADING_TEI)
        assert len(doc.sections) == 1
        assert doc.sections[0].heading == ""
        assert len(doc.sections[0].paragraphs) == 1

    def test_parse_headless_div(self):
        """Div without <head> produces section with empty heading."""
        doc = parse_tei(HEADLESS_DIV_TEI)
        assert len(doc.sections) == 1
        assert doc.sections[0].heading == ""
        assert len(doc.sections[0].paragraphs) == 2

    def test_parse_inline_refs(self):
        """<ref type='bibr'> elements in paragraphs become InlineRef."""
        doc = parse_tei(FULL_TEI)
        intro = doc.sections[0]
        # First paragraph has [1] ref
        assert len(intro.paragraphs[0].refs) == 1
        assert intro.paragraphs[0].refs[0].text == "[1]"
        assert intro.paragraphs[0].refs[0].target == "#b0"
        # Second paragraph has (Jones, 2020) ref
        assert len(intro.paragraphs[1].refs) == 1
        assert intro.paragraphs[1].refs[0].text == "(Jones, 2020)"

    def test_parse_figures(self):
        """<figure> elements with head, figDesc, graphic."""
        doc = parse_tei(FULL_TEI)
        results = doc.sections[2]
        assert len(results.figures) == 1
        fig = results.figures[0]
        assert "Figure 1" in fig.label
        assert "Expression levels" in fig.caption
        assert fig.graphic_url == "fig1.png"

    def test_parse_tables(self):
        """<figure type='table'> with row/cell structure."""
        doc = parse_tei(TABLE_TEI)
        results = doc.sections[0]
        assert len(results.tables) == 1
        table = results.tables[0]
        assert "Table 1" in table.label
        assert "Summary" in table.caption
        assert len(table.rows) == 3
        assert table.rows[0][0].text == "Gene"
        assert table.rows[1][0].text == "BRCA1"
        assert table.rows[2][2].text == "0.05"

    def test_parse_formulas(self):
        """<formula> elements."""
        doc = parse_tei(FORMULA_LIST_TEI)
        section = doc.sections[0]
        assert len(section.formulas) == 1
        assert "E = mc^2" in section.formulas[0].text

    def test_parse_lists(self):
        """<list><item> elements."""
        doc = parse_tei(FORMULA_LIST_TEI)
        section = doc.sections[0]
        assert len(section.lists) == 1
        lst = section.lists[0]
        assert len(lst.items) == 3
        assert "collect data" in lst.items[0]
        assert "analyze results" in lst.items[1]

    def test_parse_footnotes(self):
        """<note place='foot'> elements."""
        doc = parse_tei(FULL_TEI)
        discussion = doc.sections[3]
        assert len(discussion.notes) == 1
        assert "methodological details" in discussion.notes[0]

    def test_parse_acknowledgments(self):
        """Back matter acknowledgement section."""
        doc = parse_tei(FULL_TEI)
        assert "NIH" in doc.acknowledgments

    def test_parse_annex(self):
        """Back matter annex sections."""
        doc = parse_tei(FULL_TEI)
        assert len(doc.back_matter) >= 1
        assert any("Author Contributions" in s.heading for s in doc.back_matter)

    def test_parse_bibliography(self):
        """biblStruct entries with authors, title, journal, volume, pages, year, doi."""
        doc = parse_tei(FULL_TEI)
        assert len(doc.references) == 2
        ref0 = doc.references[0]
        assert "Lee" in ref0.authors[0]
        assert ref0.title == "Genomic analysis of expression"
        assert ref0.journal == "Nature Genetics"
        assert ref0.volume == "52"
        assert ref0.issue == "3"
        assert ref0.pages == "100-110"
        assert ref0.year == "2020"
        assert ref0.doi == "10.1038/ng.2020"
        ref1 = doc.references[1]
        assert "Wang" in ref1.authors[0]
        assert ref1.doi == ""  # no DOI for second ref

    def test_parse_mixed_content_paragraph(self):
        """Paragraph with interleaved text and <ref> elements."""
        doc = parse_tei(FULL_TEI)
        intro = doc.sections[0]
        p = intro.paragraphs[0]
        # Full text should contain both the text and ref text
        assert "fundamental" in p.text
        assert "[1]" in p.text
        assert "biology" in p.text

    def test_source_format_set(self):
        """Document.source_format is 'tei'."""
        doc = parse_tei(FULL_TEI)
        assert doc.source_format == "tei"

    def test_parse_hi_formatting(self):
        """<hi rend="..."> elements preserved as markdown formatting."""
        tei = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<TEI xml:space="preserve" xmlns="http://www.tei-c.org/ns/1.0">
  <teiHeader xml:lang="en">
    <fileDesc>
      <titleStmt><title level="a" type="main">T</title></titleStmt>
      <publicationStmt><publisher/></publicationStmt>
      <sourceDesc><biblStruct><analytic>
        <title level="a" type="main">T</title>
      </analytic><monogr><imprint><date when="2024"/>
      </imprint></monogr></biblStruct></sourceDesc>
    </fileDesc><profileDesc/>
  </teiHeader>
  <text xml:lang="en"><body>
    <div xmlns="http://www.tei-c.org/ns/1.0">
      <head n="1">Results</head>
      <p>The gene <hi rend="italic">drosophila</hi> has
      <hi rend="bold">significant</hi> expression of
      Ca<hi rend="superscript">2+</hi> and
      H<hi rend="subscript">2</hi>O.</p>
    </div>
  </body></text>
</TEI>
"""
        doc = parse_tei(tei)
        para = doc.sections[0].paragraphs[0].text
        assert "*drosophila*" in para
        assert "**significant**" in para
        assert "<sup>2+</sup>" in para
        assert "<sub>2</sub>" in para

    def test_parse_author_middle_name(self):
        """All forename parts (first + middle) collected."""
        tei = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<TEI xml:space="preserve" xmlns="http://www.tei-c.org/ns/1.0">
  <teiHeader xml:lang="en">
    <fileDesc>
      <titleStmt><title level="a" type="main">T</title></titleStmt>
      <publicationStmt><publisher/></publicationStmt>
      <sourceDesc>
        <biblStruct><analytic>
          <title level="a" type="main">T</title>
          <author>
            <persName>
              <forename type="first">John</forename>
              <forename type="middle">Q</forename>
              <surname>Public</surname>
            </persName>
          </author>
        </analytic><monogr><imprint><date when="2024"/>
        </imprint></monogr></biblStruct>
      </sourceDesc>
    </fileDesc><profileDesc/>
  </teiHeader>
  <text xml:lang="en"><body>
    <div xmlns="http://www.tei-c.org/ns/1.0">
      <head>I</head><p>X.</p>
    </div>
  </body></text>
</TEI>
"""
        doc = parse_tei(tei)
        assert doc.authors[0].given_name == "John Q"

    def test_parse_author_orcid(self):
        """ORCID from <idno type="ORCID">."""
        tei = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<TEI xml:space="preserve" xmlns="http://www.tei-c.org/ns/1.0">
  <teiHeader xml:lang="en">
    <fileDesc>
      <titleStmt><title level="a" type="main">T</title></titleStmt>
      <publicationStmt><publisher/></publicationStmt>
      <sourceDesc>
        <biblStruct><analytic>
          <title level="a" type="main">T</title>
          <author>
            <persName>
              <forename type="first">Jane</forename>
              <surname>Doe</surname>
            </persName>
            <idno type="ORCID">0000-0001-2345-6789</idno>
          </author>
        </analytic><monogr><imprint><date when="2024"/>
        </imprint></monogr></biblStruct>
      </sourceDesc>
    </fileDesc><profileDesc/>
  </teiHeader>
  <text xml:lang="en"><body>
    <div xmlns="http://www.tei-c.org/ns/1.0">
      <head>I</head><p>X.</p>
    </div>
  </body></text>
</TEI>
"""
        doc = parse_tei(tei)
        assert doc.authors[0].orcid == "0000-0001-2345-6789"

    def test_parse_bib_pmid(self):
        """PMID captured from <idno type="PMID">."""
        tei = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<TEI xml:space="preserve" xmlns="http://www.tei-c.org/ns/1.0">
  <teiHeader xml:lang="en">
    <fileDesc>
      <titleStmt><title level="a" type="main">T</title></titleStmt>
      <publicationStmt><publisher/></publicationStmt>
      <sourceDesc><biblStruct><analytic>
        <title level="a" type="main">T</title>
      </analytic><monogr><imprint><date when="2024"/>
      </imprint></monogr></biblStruct></sourceDesc>
    </fileDesc><profileDesc/>
  </teiHeader>
  <text xml:lang="en"><body>
    <div xmlns="http://www.tei-c.org/ns/1.0">
      <head>I</head><p>X.</p>
    </div>
  </body>
  <back><div type="references"><listBibl>
    <biblStruct>
      <analytic>
        <title level="a">Ref title</title>
        <author><persName>
          <forename type="first">A</forename>
          <surname>Ref</surname>
        </persName></author>
      </analytic>
      <monogr><title level="j">J</title>
        <imprint><date when="2023"/></imprint>
      </monogr>
      <idno type="DOI">10.1234/test</idno>
      <idno type="PMID">99887766</idno>
    </biblStruct>
  </listBibl></div></back></text>
</TEI>
"""
        doc = parse_tei(tei)
        assert doc.references[0].doi == "10.1234/test"
        assert doc.references[0].pmid == "99887766"

    def test_parse_bib_ptr(self):
        """<ptr> URLs captured in ext_links."""
        tei = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<TEI xml:space="preserve" xmlns="http://www.tei-c.org/ns/1.0">
  <teiHeader xml:lang="en">
    <fileDesc>
      <titleStmt><title level="a" type="main">T</title></titleStmt>
      <publicationStmt><publisher/></publicationStmt>
      <sourceDesc><biblStruct><analytic>
        <title level="a" type="main">T</title>
      </analytic><monogr><imprint><date when="2024"/>
      </imprint></monogr></biblStruct></sourceDesc>
    </fileDesc><profileDesc/>
  </teiHeader>
  <text xml:lang="en"><body>
    <div xmlns="http://www.tei-c.org/ns/1.0">
      <head>I</head><p>X.</p>
    </div>
  </body>
  <back><div type="references"><listBibl>
    <biblStruct>
      <analytic>
        <title level="a">Ref</title>
        <ptr target="https://doi.org/10.1234/test"/>
      </analytic>
      <monogr><title level="j">J</title>
        <imprint><date when="2023"/></imprint>
      </monogr>
    </biblStruct>
  </listBibl></div></back></text>
</TEI>
"""
        doc = parse_tei(tei)
        assert "https://doi.org/10.1234/test" in doc.references[0].ext_links

    def test_parse_monograph_title(self):
        """Book/proceedings title from <title level="m"> as journal."""
        tei = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<TEI xml:space="preserve" xmlns="http://www.tei-c.org/ns/1.0">
  <teiHeader xml:lang="en">
    <fileDesc>
      <titleStmt><title level="a" type="main">T</title></titleStmt>
      <publicationStmt><publisher/></publicationStmt>
      <sourceDesc><biblStruct><analytic>
        <title level="a" type="main">T</title>
      </analytic><monogr><imprint><date when="2024"/>
      </imprint></monogr></biblStruct></sourceDesc>
    </fileDesc><profileDesc/>
  </teiHeader>
  <text xml:lang="en"><body>
    <div xmlns="http://www.tei-c.org/ns/1.0">
      <head>I</head><p>X.</p>
    </div>
  </body>
  <back><div type="references"><listBibl>
    <biblStruct>
      <analytic>
        <title level="a">Chapter title</title>
      </analytic>
      <monogr>
        <title level="m">Current Protocols in Molecular Biology</title>
        <imprint><date when="2021"/></imprint>
      </monogr>
    </biblStruct>
  </listBibl></div></back></text>
</TEI>
"""
        doc = parse_tei(tei)
        assert doc.references[0].journal == "Current Protocols in Molecular Biology"

    def test_parse_funding_section(self):
        """<div type='funding'> in back matter captured."""
        tei = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<TEI xml:space="preserve" xmlns="http://www.tei-c.org/ns/1.0">
  <teiHeader xml:lang="en">
    <fileDesc>
      <titleStmt><title level="a" type="main">T</title></titleStmt>
      <publicationStmt><publisher/></publicationStmt>
      <sourceDesc><biblStruct><analytic>
        <title level="a" type="main">T</title>
      </analytic><monogr><imprint><date when="2024"/>
      </imprint></monogr></biblStruct></sourceDesc>
    </fileDesc><profileDesc/>
  </teiHeader>
  <text xml:lang="en"><body>
    <div xmlns="http://www.tei-c.org/ns/1.0">
      <head>I</head><p>X.</p>
    </div>
  </body>
  <back>
    <div type="funding">
      <p>This work was supported by the NIH (Grant R01).</p>
    </div>
    <div type="references"><listBibl/></div>
  </back></text>
</TEI>
"""
        doc = parse_tei(tei)
        headings = [s.heading for s in doc.back_matter]
        assert "Funding" in headings
        funding = [s for s in doc.back_matter if s.heading == "Funding"][0]
        assert "NIH" in funding.paragraphs[0].text

    def test_parse_table_footnote(self):
        """<note> in <figure type='table'> appended to caption."""
        tei = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<TEI xml:space="preserve" xmlns="http://www.tei-c.org/ns/1.0">
  <teiHeader xml:lang="en">
    <fileDesc>
      <titleStmt><title level="a" type="main">T</title></titleStmt>
      <publicationStmt><publisher/></publicationStmt>
      <sourceDesc><biblStruct><analytic>
        <title level="a" type="main">T</title>
      </analytic><monogr><imprint><date when="2024"/>
      </imprint></monogr></biblStruct></sourceDesc>
    </fileDesc><profileDesc/>
  </teiHeader>
  <text xml:lang="en"><body>
    <figure type="table" xmlns="http://www.tei-c.org/ns/1.0">
      <head>Table 1</head>
      <figDesc>Primer sequences.</figDesc>
      <table>
        <row role="head"><cell>Gene</cell><cell>Seq</cell></row>
        <row><cell>BRCA1</cell><cell>ATCG</cell></row>
      </table>
      <note>Abbreviations: fw, forward; rev, reverse.</note>
    </figure>
  </body></text>
</TEI>
"""
        doc = parse_tei(tei)
        table = doc.tables[0]
        assert "Abbreviations" in table.caption
