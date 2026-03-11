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

STRUCTURED_ABSTRACT_JATS = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<article article-type="research-article">
  <front>
    <article-meta>
      <title-group>
        <article-title>Paper With Structured Abstract</article-title>
      </title-group>
      <abstract>
        <sec>
          <title>Background</title>
          <p>Background paragraph text.</p>
        </sec>
        <sec>
          <title>Results</title>
          <p>Results paragraph text.</p>
        </sec>
        <sec>
          <title>Conclusions</title>
          <p>Conclusions paragraph text.</p>
        </sec>
      </abstract>
    </article-meta>
  </front>
  <body>
    <sec><title>Intro</title><p>Body.</p></sec>
  </body>
  <back><ref-list/></back>
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

    def test_parse_table_colspan(self):
        """Table cells with colspan emit padding cells."""
        jats = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<article><front><article-meta>
  <title-group><article-title>T</article-title></title-group>
</article-meta></front>
<body><sec><title>R</title>
  <table-wrap><table>
    <thead><tr><th colspan="2">Spanning Header</th><th>C</th></tr></thead>
    <tbody><tr><td>A</td><td>B</td><td>C</td></tr></tbody>
  </table></table-wrap>
</sec></body></article>
"""
        doc = parse_jats(jats)
        table = doc.sections[0].tables[0]
        # Header row should have 3 cells (1 real + 1 padding + 1 regular)
        assert len(table.rows[0]) == 3
        assert table.rows[0][0].text == "Spanning Header"
        assert table.rows[0][1].text == ""
        assert table.rows[0][2].text == "C"

    def test_parse_structured_abstract(self):
        """Structured abstract with <sec> preserves section titles."""
        doc = parse_jats(STRUCTURED_ABSTRACT_JATS)
        assert len(doc.abstract) == 3
        assert doc.abstract[0].text == "**Background:** Background paragraph text."
        assert doc.abstract[1].text == "**Results:** Results paragraph text."
        assert doc.abstract[2].text == "**Conclusions:** Conclusions paragraph text."

    def test_parse_mixed_citation(self):
        """References using <mixed-citation> instead of <element-citation>."""
        doc = parse_jats(MIXED_CITATION_JATS)
        assert len(doc.references) == 1
        ref = doc.references[0]
        assert "Mixed" in ref.authors[0]
        assert ref.title == "Mixed citation title"
        assert ref.journal == "Mixed Journal"
        assert ref.year == "2021"

    def test_parse_table_wrap_inside_p(self):
        """<table-wrap> nested inside <p> is extracted as a table."""
        jats = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<article><front><article-meta>
  <title-group><article-title>T</article-title></title-group>
</article-meta></front>
<body><sec><title>Results</title>
  <p>See Table 1 below.
    <table-wrap><label>Table 1</label>
      <caption><p>Summary stats.</p></caption>
      <table>
        <thead><tr><th>Gene</th><th>Value</th></tr></thead>
        <tbody><tr><td>BRCA1</td><td>2.5</td></tr></tbody>
      </table>
    </table-wrap>
  More text after table.</p>
</sec></body></article>
"""
        doc = parse_jats(jats)
        sec = doc.sections[0]
        # Table extracted from <p>
        assert len(sec.tables) == 1
        assert sec.tables[0].label == "Table 1"
        assert sec.tables[0].rows[0][0].text == "Gene"
        assert sec.tables[0].rows[1][0].text == "BRCA1"
        # Surrounding paragraph text preserved (block elements stripped)
        assert len(sec.paragraphs) == 1
        assert "See Table 1" in sec.paragraphs[0].text
        assert "More text" in sec.paragraphs[0].text

    def test_parse_citation_alternatives(self):
        """References wrapped in <citation-alternatives>."""
        jats = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<article><front><article-meta>
  <title-group><article-title>T</article-title></title-group>
</article-meta></front>
<body><sec><title>I</title><p>Text.</p></sec></body>
<back><ref-list>
  <ref id="r1">
    <citation-alternatives>
      <element-citation publication-type="journal">
        <person-group><name>
          <surname>Alt</surname><given-names>A</given-names>
        </name></person-group>
        <article-title>Alt title</article-title>
        <source>Alt Journal</source>
        <year>2023</year>
      </element-citation>
    </citation-alternatives>
  </ref>
</ref-list></back></article>
"""
        doc = parse_jats(jats)
        assert len(doc.references) == 1
        assert doc.references[0].title == "Alt title"
        assert "Alt" in doc.references[0].authors[0]

    def test_parse_inline_formatting(self):
        """Inline <italic>, <bold>, <sup>, <sub> preserved as markdown."""
        jats = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<article><front><article-meta>
  <title-group><article-title>T</article-title></title-group>
</article-meta></front>
<body><sec><title>R</title>
  <p>The gene <italic>drosophila</italic> is <bold>important</bold> for
  H<sub>2</sub>O and Ca<sup>2+</sup> studies.</p>
</sec></body></article>
"""
        doc = parse_jats(jats)
        para = doc.sections[0].paragraphs[0].text
        assert "*drosophila*" in para
        assert "**important**" in para
        assert "<sub>2</sub>" in para
        assert "<sup>2+</sup>" in para

    def test_parse_ext_link_in_paragraph(self):
        """<ext-link> in paragraphs emitted as markdown links."""
        jats = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<article><front><article-meta>
  <title-group><article-title>T</article-title></title-group>
</article-meta></front>
<body><sec><title>R</title>
  <p>Data at <ext-link ext-link-type="uri"
    xlink:href="https://example.com/data"
    xmlns:xlink="http://www.w3.org/1999/xlink"
    >example.com/data</ext-link>.</p>
</sec></body></article>
"""
        doc = parse_jats(jats)
        para = doc.sections[0].paragraphs[0].text
        assert "[example.com/data](https://example.com/data)" in para

    def test_parse_string_name_authors(self):
        """References with <string-name> instead of <name>."""
        jats = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<article><front><article-meta>
  <title-group><article-title>T</article-title></title-group>
</article-meta></front>
<body><sec><title>I</title><p>X.</p></sec></body>
<back><ref-list>
  <ref id="r1">
    <mixed-citation>
      <string-name><surname>Sn</surname><given-names>A</given-names></string-name>
      <article-title>SN title</article-title>
      <source>SN Journal</source>
      <year>2022</year>
    </mixed-citation>
  </ref>
</ref-list></back></article>
"""
        doc = parse_jats(jats)
        assert "Sn" in doc.references[0].authors[0]

    def test_parse_elocation_id(self):
        """References with <elocation-id> instead of fpage/lpage."""
        jats = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<article><front><article-meta>
  <title-group><article-title>T</article-title></title-group>
</article-meta></front>
<body><sec><title>I</title><p>X.</p></sec></body>
<back><ref-list>
  <ref id="r1">
    <element-citation>
      <article-title>E-journal paper</article-title>
      <source>PLOS ONE</source>
      <year>2023</year>
      <elocation-id>e12345</elocation-id>
    </element-citation>
  </ref>
</ref-list></back></article>
"""
        doc = parse_jats(jats)
        assert doc.references[0].pages == "e12345"

    def test_parse_pmcid(self):
        """PMCID captured from pub-id[@pub-id-type='pmcid']."""
        jats = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<article><front><article-meta>
  <title-group><article-title>T</article-title></title-group>
</article-meta></front>
<body><sec><title>I</title><p>X.</p></sec></body>
<back><ref-list>
  <ref id="r1">
    <element-citation>
      <article-title>Title</article-title>
      <year>2024</year>
      <pub-id pub-id-type="doi">10.1234/test</pub-id>
      <pub-id pub-id-type="pmid">99999999</pub-id>
      <pub-id pub-id-type="pmcid">PMC1234567</pub-id>
    </element-citation>
  </ref>
</ref-list></back></article>
"""
        doc = parse_jats(jats)
        ref = doc.references[0]
        assert ref.doi == "10.1234/test"
        assert ref.pmid == "99999999"
        assert ref.pmcid == "PMC1234567"

    def test_parse_collab_author(self):
        """Collaborative/group author names."""
        jats = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<article><front><article-meta>
  <title-group><article-title>T</article-title></title-group>
</article-meta></front>
<body><sec><title>I</title><p>X.</p></sec></body>
<back><ref-list>
  <ref id="r1">
    <element-citation>
      <person-group>
        <collab>The International Consortium</collab>
      </person-group>
      <article-title>Consortium paper</article-title>
      <year>2024</year>
    </element-citation>
  </ref>
</ref-list></back></article>
"""
        doc = parse_jats(jats)
        assert "The International Consortium" in doc.references[0].authors

    def test_parse_author_orcid(self):
        """Author ORCID from contrib-id."""
        jats = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<article><front><article-meta>
  <title-group><article-title>T</article-title></title-group>
  <contrib-group>
    <contrib contrib-type="author">
      <contrib-id contrib-id-type="orcid">0000-0001-2345-6789</contrib-id>
      <name><surname>Orcid</surname><given-names>A</given-names></name>
    </contrib>
  </contrib-group>
</article-meta></front>
<body><sec><title>I</title><p>X.</p></sec></body></article>
"""
        doc = parse_jats(jats)
        assert doc.authors[0].orcid == "0000-0001-2345-6789"

    def test_parse_back_sections(self):
        """Back-matter sec, fn-group, notes parsed as back_matter."""
        jats = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<article><front><article-meta>
  <title-group><article-title>T</article-title></title-group>
</article-meta></front>
<body><sec><title>I</title><p>X.</p></sec></body>
<back>
  <sec sec-type="data-availability">
    <title>Data Availability</title>
    <p>Data deposited at GEO.</p>
  </sec>
  <fn-group>
    <title>Author Contributions</title>
    <fn><p>A.B. conceived the study.</p></fn>
  </fn-group>
</back></article>
"""
        doc = parse_jats(jats)
        headings = [s.heading for s in doc.back_matter]
        assert "Data Availability" in headings
        assert "Author Contributions" in headings

    def test_parse_supplementary_material(self):
        """<supplementary-material> in sec rendered as paragraph."""
        jats = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<article><front><article-meta>
  <title-group><article-title>T</article-title></title-group>
</article-meta></front>
<body><sec><title>Results</title>
  <p>Main findings.</p>
  <supplementary-material>
    <label>Supplementary File 1</label>
    <caption><p>Additional data tables.</p></caption>
  </supplementary-material>
</sec></body></article>
"""
        doc = parse_jats(jats)
        sec = doc.sections[0]
        supp_paras = [p for p in sec.paragraphs
                      if "Supplementary" in p.text]
        assert len(supp_paras) == 1
        assert "Additional data" in supp_paras[0].text

    def test_parse_disp_quote(self):
        """<disp-quote> in sec rendered as block quote."""
        jats = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<article><front><article-meta>
  <title-group><article-title>T</article-title></title-group>
</article-meta></front>
<body><sec><title>Discussion</title>
  <disp-quote><p>A famous quote here.</p></disp-quote>
</sec></body></article>
"""
        doc = parse_jats(jats)
        sec = doc.sections[0]
        quote_paras = [p for p in sec.paragraphs if p.text.startswith(">")]
        assert len(quote_paras) == 1
        assert "famous quote" in quote_paras[0].text

    def test_parse_def_list(self):
        """<def-list>/<def-item> rendered as list."""
        jats = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<article><front><article-meta>
  <title-group><article-title>T</article-title></title-group>
</article-meta></front>
<body><sec><title>Glossary</title>
  <def-list>
    <def-item>
      <term>GO</term>
      <def><p>Gene Ontology</p></def>
    </def-item>
  </def-list>
</sec></body></article>
"""
        doc = parse_jats(jats)
        sec = doc.sections[0]
        assert len(sec.lists) == 1
        assert "**GO**" in sec.lists[0].items[0]
        assert "Gene Ontology" in sec.lists[0].items[0]

    def test_parse_table_wrap_foot(self):
        """<table-wrap-foot> footnotes captured in Table.foot_notes."""
        jats = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<article><front><article-meta>
  <title-group><article-title>T</article-title></title-group>
</article-meta></front>
<body><sec><title>R</title>
  <table-wrap>
    <label>Table 1</label>
    <table>
      <thead><tr><th>Gene</th><th>FC</th></tr></thead>
      <tbody><tr><td>BRCA1</td><td>2.5</td></tr></tbody>
    </table>
    <table-wrap-foot>
      <fn id="tfn1"><p>FC, fold change.</p></fn>
      <fn id="tfn2"><p>*P &lt; 0.05.</p></fn>
    </table-wrap-foot>
  </table-wrap>
</sec></body></article>
"""
        doc = parse_jats(jats)
        table = doc.sections[0].tables[0]
        assert len(table.foot_notes) == 2
        assert "fold change" in table.foot_notes[0]
        assert "P <" in table.foot_notes[1]

    def test_parse_page_range(self):
        """<page-range> as fallback for pages in references."""
        jats = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<article><front><article-meta>
  <title-group><article-title>T</article-title></title-group>
</article-meta></front>
<body><sec><title>I</title><p>X.</p></sec></body>
<back><ref-list>
  <ref id="r1">
    <element-citation>
      <article-title>Title</article-title>
      <source>J</source>
      <year>2024</year>
      <page-range>100-110, 115</page-range>
    </element-citation>
  </ref>
</ref-list></back></article>
"""
        doc = parse_jats(jats)
        assert doc.references[0].pages == "100-110, 115"

    def test_parse_publisher_info(self):
        """<publisher-name> and <publisher-loc> in references."""
        jats = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<article><front><article-meta>
  <title-group><article-title>T</article-title></title-group>
</article-meta></front>
<body><sec><title>I</title><p>X.</p></sec></body>
<back><ref-list>
  <ref id="r1">
    <element-citation publication-type="book">
      <person-group person-group-type="author">
        <name><surname>Auth</surname><given-names>A</given-names></name>
      </person-group>
      <source>Biology Handbook</source>
      <year>2023</year>
      <publisher-name>Academic Press</publisher-name>
      <publisher-loc>New York</publisher-loc>
    </element-citation>
  </ref>
</ref-list></back></article>
"""
        doc = parse_jats(jats)
        ref = doc.references[0]
        assert ref.publisher == "Academic Press"
        assert ref.publisher_loc == "New York"

    def test_parse_chapter_title(self):
        """<chapter-title> / <part-title> for book chapters."""
        jats = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<article><front><article-meta>
  <title-group><article-title>T</article-title></title-group>
</article-meta></front>
<body><sec><title>I</title><p>X.</p></sec></body>
<back><ref-list>
  <ref id="r1">
    <element-citation publication-type="book">
      <article-title>Chapter One</article-title>
      <chapter-title>Methods in Molecular Biology</chapter-title>
      <source>Book Title</source>
      <year>2022</year>
    </element-citation>
  </ref>
</ref-list></back></article>
"""
        doc = parse_jats(jats)
        assert doc.references[0].chapter_title == "Methods in Molecular Biology"

    def test_parse_conf_name(self):
        """<conf-name> captured as conference."""
        jats = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<article><front><article-meta>
  <title-group><article-title>T</article-title></title-group>
</article-meta></front>
<body><sec><title>I</title><p>X.</p></sec></body>
<back><ref-list>
  <ref id="r1">
    <element-citation publication-type="confproc">
      <article-title>Deep learning for genomics</article-title>
      <conf-name>ISMB 2024</conf-name>
      <year>2024</year>
    </element-citation>
  </ref>
</ref-list></back></article>
"""
        doc = parse_jats(jats)
        assert doc.references[0].conference == "ISMB 2024"

    def test_parse_ref_editors(self):
        """Editors from person-group[@person-group-type='editor']."""
        jats = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<article><front><article-meta>
  <title-group><article-title>T</article-title></title-group>
</article-meta></front>
<body><sec><title>I</title><p>X.</p></sec></body>
<back><ref-list>
  <ref id="r1">
    <element-citation publication-type="book">
      <person-group person-group-type="author">
        <name><surname>Auth</surname><given-names>A</given-names></name>
      </person-group>
      <person-group person-group-type="editor">
        <name><surname>Editor</surname><given-names>E</given-names></name>
      </person-group>
      <article-title>Chapter</article-title>
      <source>Big Book</source>
      <year>2023</year>
    </element-citation>
  </ref>
</ref-list></back></article>
"""
        doc = parse_jats(jats)
        ref = doc.references[0]
        assert len(ref.editors) == 1
        assert "Editor E" in ref.editors[0]

    def test_parse_preformat(self):
        """<preformat> blocks rendered as code blocks."""
        jats = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<article><front><article-meta>
  <title-group><article-title>T</article-title></title-group>
</article-meta></front>
<body><sec><title>Methods</title>
  <preformat>SELECT * FROM genes WHERE symbol = 'BRCA1';</preformat>
</sec></body></article>
"""
        doc = parse_jats(jats)
        sec = doc.sections[0]
        code_paras = [p for p in sec.paragraphs if "```" in p.text]
        assert len(code_paras) == 1
        assert "SELECT * FROM genes" in code_paras[0].text

    def test_parse_glossary_in_back(self):
        """<glossary> in back matter parsed with def-list."""
        jats = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<article><front><article-meta>
  <title-group><article-title>T</article-title></title-group>
</article-meta></front>
<body><sec><title>I</title><p>X.</p></sec></body>
<back>
  <glossary>
    <title>Abbreviations</title>
    <def-list>
      <def-item>
        <term>GO</term>
        <def><p>Gene Ontology</p></def>
      </def-item>
      <def-item>
        <term>MOD</term>
        <def><p>Model Organism Database</p></def>
      </def-item>
    </def-list>
  </glossary>
</back></article>
"""
        doc = parse_jats(jats)
        headings = [s.heading for s in doc.back_matter]
        assert "Abbreviations" in headings
        abbr = [s for s in doc.back_matter
                if s.heading == "Abbreviations"][0]
        assert len(abbr.lists) >= 1
        assert "**GO**" in abbr.lists[0].items[0]

    def test_parse_empty_inline_formatting(self):
        """Empty <italic>/<bold>/<sup>/<sub> produce no stray markers."""
        jats = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<article><front><article-meta>
  <title-group><article-title>T</article-title></title-group>
</article-meta></front>
<body><sec><title>R</title>
  <p>Before<italic></italic> middle<bold></bold> after <sup></sup> end<sub></sub>.</p>
</sec></body></article>
"""
        doc = parse_jats(jats)
        para = doc.sections[0].paragraphs[0].text
        assert "**" not in para
        assert "<sup></sup>" not in para
        assert "<sub></sub>" not in para
        assert "Before" in para
        assert "middle" in para

    def test_parse_nested_inline_formatting(self):
        """Nested inline markup: <italic>text <sup>x</sup></italic>."""
        jats = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<article><front><article-meta>
  <title-group><article-title>T</article-title></title-group>
</article-meta></front>
<body><sec><title>R</title>
  <p>The gene <italic>Drosophila <sup>x</sup></italic> is studied.</p>
</sec></body></article>
"""
        doc = parse_jats(jats)
        para = doc.sections[0].paragraphs[0].text
        assert "*Drosophila <sup>x</sup>*" in para

    def test_parse_rowspan_warning(self, caplog):
        """rowspan on table cells logs a warning."""
        jats = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<article><front><article-meta>
  <title-group><article-title>T</article-title></title-group>
</article-meta></front>
<body><sec><title>R</title>
  <table-wrap><table>
    <thead><tr><th>A</th><th>B</th></tr></thead>
    <tbody>
      <tr><td rowspan="2">Spanning</td><td>1</td></tr>
      <tr><td>2</td></tr>
    </tbody>
  </table></table-wrap>
</sec></body></article>
"""
        import logging
        with caplog.at_level(logging.WARNING):
            doc = parse_jats(jats)
        assert any("rowspan" in r.message for r in caplog.records)
        # Table still parsed (just potentially misaligned)
        assert len(doc.sections[0].tables[0].rows) >= 2

    def test_parse_ref_author_editor_separation(self):
        """Editors not captured as authors in refs with both groups."""
        jats = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<article><front><article-meta>
  <title-group><article-title>T</article-title></title-group>
</article-meta></front>
<body><sec><title>I</title><p>X.</p></sec></body>
<back><ref-list>
  <ref id="r1">
    <element-citation publication-type="book">
      <person-group person-group-type="author">
        <name><surname>Writer</surname><given-names>A</given-names></name>
      </person-group>
      <person-group person-group-type="editor">
        <name><surname>Editor</surname><given-names>E</given-names></name>
      </person-group>
      <source>Big Book</source>
      <year>2023</year>
    </element-citation>
  </ref>
</ref-list></back></article>
"""
        doc = parse_jats(jats)
        ref = doc.references[0]
        assert len(ref.authors) == 1
        assert "Writer" in ref.authors[0]
        assert "Editor" not in ref.authors[0]
        assert len(ref.editors) == 1
        assert "Editor" in ref.editors[0]

    def test_parse_preformat_with_backticks(self):
        """<preformat> containing backticks uses wider fence."""
        jats = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<article><front><article-meta>
  <title-group><article-title>T</article-title></title-group>
</article-meta></front>
<body><sec><title>Methods</title>
  <preformat>Run ```this``` command</preformat>
</sec></body></article>
"""
        doc = parse_jats(jats)
        sec = doc.sections[0]
        code_paras = [p for p in sec.paragraphs if "Run" in p.text]
        assert len(code_paras) == 1
        # Fence should be wider than 3 backticks
        assert "````" in code_paras[0].text

    def test_parse_disp_quote_multi_paragraph(self):
        """<disp-quote> with multiple <p> children gets per-paragraph >."""
        jats = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<article><front><article-meta>
  <title-group><article-title>T</article-title></title-group>
</article-meta></front>
<body><sec><title>Discussion</title>
  <disp-quote>
    <p>First paragraph of quote.</p>
    <p>Second paragraph of quote.</p>
  </disp-quote>
</sec></body></article>
"""
        doc = parse_jats(jats)
        sec = doc.sections[0]
        quote_paras = [p for p in sec.paragraphs if p.text.startswith(">")]
        assert len(quote_paras) == 1
        text = quote_paras[0].text
        assert "> First paragraph of quote." in text
        assert "> Second paragraph of quote." in text
        # Each paragraph gets its own > prefix
        assert text.count(">") == 2

    def test_parse_glossary_no_title_duplication(self):
        """Glossary title in back matter not duplicated as bold paragraph."""
        jats = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<article><front><article-meta>
  <title-group><article-title>T</article-title></title-group>
</article-meta></front>
<body><sec><title>I</title><p>X.</p></sec></body>
<back>
  <glossary>
    <title>Abbreviations</title>
    <def-list>
      <def-item>
        <term>GO</term>
        <def><p>Gene Ontology</p></def>
      </def-item>
    </def-list>
  </glossary>
</back></article>
"""
        doc = parse_jats(jats)
        abbr = [s for s in doc.back_matter
                if s.heading == "Abbreviations"][0]
        # Title should be the section heading, not also a bold paragraph
        bold_titles = [p for p in abbr.paragraphs
                       if "**Abbreviations**" in p.text]
        assert len(bold_titles) == 0

    def test_parse_whitespace_normalization(self):
        """XML indentation whitespace collapsed in paragraph text."""
        jats = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<article><front><article-meta>
  <title-group><article-title>T</article-title></title-group>
</article-meta></front>
<body><sec><title>R</title>
  <p>Text with
    <xref ref-type="bibr" rid="r1">[1]</xref>
    and more text.</p>
</sec></body></article>
"""
        doc = parse_jats(jats)
        para = doc.sections[0].paragraphs[0].text
        assert "\n" not in para
        assert "  " not in para
        assert "Text with [1] and more text." == para
