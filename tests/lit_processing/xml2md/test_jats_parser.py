"""Tests for JATS/nXML parser."""

from agr_literature_service.lit_processing.xml2md.jats_parser import parse_jats


# -- JATS XML test fixtures --------------------------------------------------

FULL_JATS = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE article PUBLIC "-//NLM//DTD JATS (Z39.96) Journal Archiving
  and Interchange Tag Set v1.0 20120330//EN"
  "JATS-archivearticle1.dtd">
<article article-type="research-article">
  <front>
    <journal-meta>
      <journal-id journal-id-type="nlm-ta">Nat Genet</journal-id>
      <journal-title-group>
        <journal-title>Nature Genetics</journal-title>
      </journal-title-group>
    </journal-meta>
    <article-meta>
      <article-id pub-id-type="pmid">12345678</article-id>
      <article-id pub-id-type="doi">10.1038/ng.test.2024</article-id>
      <article-id pub-id-type="pmc">PMC9999999</article-id>
      <title-group>
        <article-title>Genomic Analysis of Model Organisms</article-title>
      </title-group>
      <contrib-group>
        <contrib contrib-type="author">
          <name><surname>Smith</surname><given-names>Alice M</given-names></name>
          <email>alice@example.com</email>
          <xref ref-type="aff" rid="aff1"/>
        </contrib>
        <contrib contrib-type="author">
          <name><surname>Jones</surname><given-names>Bob</given-names></name>
          <xref ref-type="aff" rid="aff2"/>
        </contrib>
      </contrib-group>
      <aff id="aff1">Department of Biology, MIT, Cambridge, MA, USA</aff>
      <aff id="aff2">Department of CS, Stanford University, Stanford, CA, USA</aff>
      <pub-date pub-type="epub"><year>2024</year></pub-date>
      <volume>56</volume>
      <issue>4</issue>
      <fpage>300</fpage>
      <lpage>315</lpage>
      <kwd-group>
        <kwd>genomics</kwd>
        <kwd>model organisms</kwd>
        <kwd>comparative analysis</kwd>
      </kwd-group>
      <abstract>
        <p>We performed a comprehensive genomic analysis of model organisms.</p>
        <p>Our results reveal conserved regulatory elements.</p>
      </abstract>
    </article-meta>
  </front>
  <body>
    <sec id="sec1">
      <title>Introduction</title>
      <p>Model organisms are essential for <xref ref-type="bibr" rid="ref1">[1]</xref> genomic research.</p>
      <p>Previous studies have shown <xref ref-type="bibr" rid="ref2">[2]</xref> that...</p>
    </sec>
    <sec id="sec2">
      <title>Methods</title>
      <p>We collected samples from multiple organisms.</p>
      <sec id="sec2.1">
        <title>Sample Collection</title>
        <p>Samples were obtained from standard repositories.</p>
      </sec>
      <sec id="sec2.2">
        <title>Sequencing</title>
        <p>Whole-genome sequencing was performed using Illumina.</p>
      </sec>
    </sec>
    <sec id="sec3">
      <title>Results</title>
      <p>We identified significant genomic conservation.</p>
      <fig id="fig1">
        <label>Figure 1</label>
        <caption><title>Genomic conservation</title><p>Conservation scores across species.</p></caption>
        <graphic xlink:href="fig1.tif" xmlns:xlink="http://www.w3.org/1999/xlink"/>
      </fig>
      <table-wrap id="tab1">
        <label>Table 1</label>
        <caption><title>Species comparison</title><p>Key metrics by species.</p></caption>
        <table>
          <thead>
            <tr><th>Species</th><th>Genes</th><th>Conservation</th></tr>
          </thead>
          <tbody>
            <tr><td>C. elegans</td><td>20000</td><td>0.85</td></tr>
            <tr><td>D. melanogaster</td><td>14000</td><td>0.78</td></tr>
          </tbody>
        </table>
      </table-wrap>
      <disp-formula id="eq1">E = mc^2</disp-formula>
      <list list-type="bullet">
        <list-item><p>Finding one: conservation is high</p></list-item>
        <list-item><p>Finding two: regulatory elements shared</p></list-item>
      </list>
    </sec>
  </body>
  <back>
    <ack><p>We thank the genome sequencing centers for data access.</p></ack>
    <ref-list>
      <ref id="ref1">
        <element-citation publication-type="journal">
          <person-group person-group-type="author">
            <name><surname>Lee</surname><given-names>C</given-names></name>
            <name><surname>Park</surname><given-names>D</given-names></name>
          </person-group>
          <article-title>Comparative genomics review</article-title>
          <source>Annual Review of Genomics</source>
          <year>2020</year>
          <volume>21</volume>
          <issue>1</issue>
          <fpage>50</fpage>
          <lpage>75</lpage>
          <pub-id pub-id-type="doi">10.1146/annurev.2020</pub-id>
          <pub-id pub-id-type="pmid">11111111</pub-id>
        </element-citation>
      </ref>
      <ref id="ref2">
        <element-citation publication-type="journal">
          <person-group person-group-type="author">
            <name><surname>Wang</surname><given-names>E</given-names></name>
          </person-group>
          <article-title>Model organism databases</article-title>
          <source>Nucleic Acids Research</source>
          <year>2019</year>
          <volume>47</volume>
          <fpage>D1</fpage>
          <lpage>D10</lpage>
        </element-citation>
      </ref>
    </ref-list>
    <app-group>
      <app>
        <title>Supplementary Methods</title>
        <p>Additional details on the sequencing protocol.</p>
      </app>
    </app-group>
  </back>
</article>
"""

NO_NAMESPACE_JATS = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<article article-type="research-article">
  <front>
    <article-meta>
      <article-id pub-id-type="doi">10.1234/no-ns</article-id>
      <title-group>
        <article-title>Paper Without Namespace</article-title>
      </title-group>
      <contrib-group>
        <contrib contrib-type="author">
          <name><surname>Doe</surname><given-names>Jane</given-names></name>
        </contrib>
      </contrib-group>
      <abstract><p>Abstract without namespace.</p></abstract>
    </article-meta>
  </front>
  <body>
    <sec>
      <title>Introduction</title>
      <p>Body text without namespace.</p>
    </sec>
  </body>
  <back>
    <ref-list/>
  </back>
</article>
"""

MIXED_CITATION_JATS = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<article article-type="research-article">
  <front>
    <article-meta>
      <title-group>
        <article-title>Paper With Mixed Citations</article-title>
      </title-group>
      <abstract><p>Test abstract.</p></abstract>
    </article-meta>
  </front>
  <body>
    <sec><title>Intro</title><p>Text.</p></sec>
  </body>
  <back>
    <ref-list>
      <ref id="r1">
        <mixed-citation publication-type="journal">
          <person-group person-group-type="author">
            <name><surname>Mixed</surname><given-names>A</given-names></name>
          </person-group>
          <article-title>Mixed citation title</article-title>
          <source>Mixed Journal</source>
          <year>2021</year>
          <volume>10</volume>
          <fpage>1</fpage>
          <lpage>5</lpage>
        </mixed-citation>
      </ref>
    </ref-list>
  </back>
</article>
"""


class TestJatsParser:
    """Tests for parse_jats function."""

    def test_parse_title(self):
        """Title from //article-meta/title-group/article-title."""
        doc = parse_jats(FULL_JATS)
        assert doc.title == "Genomic Analysis of Model Organisms"

    def test_parse_authors(self):
        """Authors from contrib-group/contrib[@contrib-type='author']."""
        doc = parse_jats(FULL_JATS)
        assert len(doc.authors) == 2
        assert doc.authors[0].given_name == "Alice M"
        assert doc.authors[0].surname == "Smith"
        assert doc.authors[0].email == "alice@example.com"
        assert len(doc.authors[0].affiliations) == 1
        assert "MIT" in doc.authors[0].affiliations[0]
        assert doc.authors[1].surname == "Jones"
        assert "Stanford" in doc.authors[1].affiliations[0]

    def test_parse_abstract(self):
        """Abstract from //article-meta/abstract."""
        doc = parse_jats(FULL_JATS)
        assert len(doc.abstract) == 2
        assert "comprehensive genomic analysis" in doc.abstract[0].text
        assert "conserved regulatory" in doc.abstract[1].text

    def test_parse_keywords(self):
        """Keywords from //kwd-group/kwd."""
        doc = parse_jats(FULL_JATS)
        assert len(doc.keywords) == 3
        assert "genomics" in doc.keywords
        assert "model organisms" in doc.keywords

    def test_parse_doi(self):
        """DOI from //article-id[@pub-id-type='doi']."""
        doc = parse_jats(FULL_JATS)
        assert doc.doi == "10.1038/ng.test.2024"

    def test_parse_body_sections(self):
        """//body/sec -> sections with <title> headings."""
        doc = parse_jats(FULL_JATS)
        assert len(doc.sections) == 3
        assert doc.sections[0].heading == "Introduction"
        assert doc.sections[1].heading == "Methods"
        assert doc.sections[2].heading == "Results"

    def test_parse_nested_sections(self):
        """Nested <sec> elements."""
        doc = parse_jats(FULL_JATS)
        methods = doc.sections[1]
        assert len(methods.subsections) == 2
        assert methods.subsections[0].heading == "Sample Collection"
        assert methods.subsections[0].level == 2
        assert methods.subsections[1].heading == "Sequencing"

    def test_parse_figures(self):
        """<fig> with label, caption."""
        doc = parse_jats(FULL_JATS)
        results = doc.sections[2]
        assert len(results.figures) == 1
        fig = results.figures[0]
        assert fig.label == "Figure 1"
        assert "conservation" in fig.caption.lower()

    def test_parse_tables(self):
        """<table-wrap> with thead/tbody/tr/th/td."""
        doc = parse_jats(FULL_JATS)
        results = doc.sections[2]
        assert len(results.tables) == 1
        table = results.tables[0]
        assert table.label == "Table 1"
        assert len(table.rows) == 3  # 1 header + 2 data rows
        assert table.rows[0][0].text == "Species"
        assert table.rows[0][0].is_header is True
        assert table.rows[1][0].text == "C. elegans"
        assert table.rows[2][1].text == "14000"

    def test_parse_formulas(self):
        """<disp-formula> elements."""
        doc = parse_jats(FULL_JATS)
        results = doc.sections[2]
        assert len(results.formulas) == 1
        assert "E = mc^2" in results.formulas[0].text

    def test_parse_lists(self):
        """<list>/<list-item> elements."""
        doc = parse_jats(FULL_JATS)
        results = doc.sections[2]
        assert len(results.lists) == 1
        lst = results.lists[0]
        assert len(lst.items) == 2
        assert "conservation" in lst.items[0].lower()

    def test_parse_acknowledgments(self):
        """//back/ack."""
        doc = parse_jats(FULL_JATS)
        assert "genome sequencing centers" in doc.acknowledgments

    def test_parse_bibliography(self):
        """//back/ref-list/ref with element-citation."""
        doc = parse_jats(FULL_JATS)
        assert len(doc.references) == 2
        ref0 = doc.references[0]
        assert "Lee" in ref0.authors[0]
        assert ref0.title == "Comparative genomics review"
        assert ref0.journal == "Annual Review of Genomics"
        assert ref0.volume == "21"
        assert ref0.issue == "1"
        assert ref0.pages == "50-75"
        assert ref0.year == "2020"
        assert ref0.doi == "10.1146/annurev.2020"
        assert ref0.pmid == "11111111"

    def test_parse_appendices(self):
        """//back/app-group -> back_matter."""
        doc = parse_jats(FULL_JATS)
        assert len(doc.back_matter) >= 1
        assert any(
            "Supplementary Methods" in s.heading
            for s in doc.back_matter
        )

    def test_source_format_set(self):
        """Document.source_format is 'jats'."""
        doc = parse_jats(FULL_JATS)
        assert doc.source_format == "jats"

    def test_parse_no_namespace(self):
        """JATS file without namespace (common variant)."""
        doc = parse_jats(NO_NAMESPACE_JATS)
        assert doc.title == "Paper Without Namespace"
        assert doc.doi == "10.1234/no-ns"
        assert len(doc.authors) == 1
        assert doc.authors[0].surname == "Doe"
        assert len(doc.abstract) == 1
        assert len(doc.sections) == 1

    def test_parse_mixed_citation(self):
        """References using <mixed-citation> instead of <element-citation>."""
        doc = parse_jats(MIXED_CITATION_JATS)
        assert len(doc.references) == 1
        ref = doc.references[0]
        assert "Mixed" in ref.authors[0]
        assert ref.title == "Mixed citation title"
        assert ref.journal == "Mixed Journal"
        assert ref.year == "2021"
