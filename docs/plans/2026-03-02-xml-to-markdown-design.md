# XML-to-Markdown Conversion Service Design

**Date:** 2026-03-02
**Status:** Approved
**Jira tickets:** KANBAN-1062, SCRUM-5797, SCRUM-5798, SCRUM-5799, SCRUM-5794

## Summary

Add a conversion service to `agr_literature_service` that converts GROBID TEI XML and PMC nXML (JATS) files into docling-style Markdown. The service includes a Python/lxml-based converter module, an on-demand API endpoint, and a batch processing script. Converted files are stored in S3 as new referencefiles.

## Context and Motivation

The Alliance is migrating from TEI XML to Markdown as the standard text representation for scientific papers. The new PDF extraction service (KANBAN-1034) produces Markdown via three extractors (GROBID, Docling, Marker) merged by an LLM. This conversion service bridges the gap by converting existing TEI and nXML files into the same Markdown format, enabling:

- Downstream ML pipelines (classification, entity extraction) to consume Markdown instead of TEI
- Consistent text format across all conversion sources
- Leveraging existing PMC nXML files to avoid costly PDF re-conversions

## File Class Values (from SCRUM-5798)

Defined in SCRUM-5798 acceptance criteria (`customfield_10622`):

**For main PDF conversions:**
- `converted_grobid_main`
- `converted_docling_main`
- `converted_marker_main`
- `converted_merged_main`

**For supplement PDF conversions:**
- `converted_grobid_supplement`
- `converted_docling_supplement`
- `converted_marker_supplement`
- `converted_merged_supplement`

**For nXML conversions (needs discussion per SCRUM-5799):**
- Proposed: `converted_nxml_main` / `converted_nxml_supplement`

All converted Markdown files use `file_extension="md"`.

## Existing file_class Values in System

| file_class | Description |
|---|---|
| `main` | Main PDF |
| `supplement` | Supplementary files |
| `tei` | GROBID TEI XML output |
| `nXML` | PMC nXML/JATS files |
| `figure` | Image files (jpg, jpeg, gif, tif, tiff, png) |
| `thumbnail` | Thumbnail images |

## Architecture

### Module Structure

```
agr_literature_service/lit_processing/xml2md/
    __init__.py
    converter.py      # Main entry point: format detection, dispatch, orchestration
    tei_parser.py     # GROBID TEI XML -> Document model
    jats_parser.py    # PMC nXML/JATS XML -> Document model
    md_emitter.py     # Document model -> docling-style Markdown string
    models.py         # Shared intermediate data model (Document, Section, etc.)
```

### Intermediate Data Model (`models.py`)

Both parsers produce the same `Document` structure, decoupling parsing from Markdown generation:

```python
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Author:
    given_name: str = ""
    surname: str = ""
    email: str = ""
    affiliations: list[str] = field(default_factory=list)


@dataclass
class InlineRef:
    """An inline citation reference like [1] or (Author, 2024)."""
    text: str = ""
    target: str = ""  # e.g., "#b12" pointing to a biblStruct


@dataclass
class Paragraph:
    text: str = ""
    refs: list[InlineRef] = field(default_factory=list)


@dataclass
class Figure:
    label: str = ""       # "Figure 1" or "Fig. 1"
    caption: str = ""     # figDesc text
    graphic_url: str = "" # optional image reference


@dataclass
class TableCell:
    text: str = ""
    is_header: bool = False


@dataclass
class Table:
    label: str = ""              # "Table 1"
    caption: str = ""
    rows: list[list[TableCell]] = field(default_factory=list)


@dataclass
class Formula:
    text: str = ""
    label: str = ""


@dataclass
class ListBlock:
    items: list[str] = field(default_factory=list)
    ordered: bool = False


@dataclass
class Section:
    heading: str = ""
    number: str = ""                  # "1", "1.1", etc. from <head n="...">
    level: int = 1                    # nesting depth -> Markdown heading level
    paragraphs: list[Paragraph] = field(default_factory=list)
    figures: list[Figure] = field(default_factory=list)
    tables: list[Table] = field(default_factory=list)
    formulas: list[Formula] = field(default_factory=list)
    lists: list[ListBlock] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    subsections: list["Section"] = field(default_factory=list)


@dataclass
class Reference:
    """A single entry in the bibliography."""
    index: int = 0
    authors: list[str] = field(default_factory=list)  # "Surname FN" format
    title: str = ""
    journal: str = ""
    volume: str = ""
    issue: str = ""
    pages: str = ""
    year: str = ""
    doi: str = ""
    pmid: str = ""


@dataclass
class Document:
    title: str = ""
    authors: list[Author] = field(default_factory=list)
    abstract: list[Paragraph] = field(default_factory=list)
    keywords: list[str] = field(default_factory=list)
    doi: str = ""
    sections: list[Section] = field(default_factory=list)
    figures: list[Figure] = field(default_factory=list)
    tables: list[Table] = field(default_factory=list)
    references: list[Reference] = field(default_factory=list)
    acknowledgments: str = ""
    back_matter: list[Section] = field(default_factory=list)
    source_format: str = ""  # "tei" or "jats"
```

### TEI Parser (`tei_parser.py`)

Uses `lxml.etree` with the TEI namespace `http://www.tei-c.org/ns/1.0`.

**Key implementation notes based on analysis of real TEI files:**

- All TEI files are GROBID v0.8.0 output with root `<TEI xml:space="preserve" xmlns="http://www.tei-c.org/ns/1.0">`
- Namespace must be used for all XPath queries: `{'tei': 'http://www.tei-c.org/ns/1.0'}`
- Section structure is NOT consistent across papers — headings, numbering, and nesting vary significantly
- Heading levels must be derived from nesting depth, not assumed section names
- Section numbers preserved from `<head n="...">` when present, omitted when absent

**Element mapping:**

| TEI XPath | Document field |
|---|---|
| `//tei:teiHeader//tei:title[@level="a"]` | `Document.title` |
| `//tei:sourceDesc//tei:author/tei:persName` | `Document.authors` (forename + surname) |
| `//tei:sourceDesc//tei:author//tei:email` | `Author.email` |
| `//tei:sourceDesc//tei:author//tei:affiliation` | `Author.affiliations` |
| `//tei:profileDesc/tei:abstract` | `Document.abstract` (handles both `<div><p>` and direct `<p>`) |
| `//tei:textClass//tei:keywords/tei:term` | `Document.keywords` |
| `//tei:sourceDesc//tei:idno[@type="DOI"]` | `Document.doi` |
| `//tei:body/tei:div` | `Document.sections` (recursive walk) |
| `tei:div/tei:head` | `Section.heading` + `Section.number` (from `@n` attr) |
| `tei:div/tei:p` | `Section.paragraphs` |
| `tei:p/tei:ref` | `InlineRef` (type + target attributes) |
| `tei:figure` (no type attr) | `Figure` with `<figDesc>` caption, `<head>` label |
| `tei:figure[@type="table"]` | `Table` with `<head>` label, `<row>/<cell>` content |
| `tei:formula` | `Formula` |
| `tei:list/tei:item` | `ListBlock` |
| `tei:note[@place="foot"]` | `Section.notes` (footnotes) |
| `//tei:back/tei:div[@type="acknowledgement"]` | `Document.acknowledgments` |
| `//tei:back/tei:div[@type="annex"]` | `Document.back_matter` |
| `//tei:back//tei:listBibl/tei:biblStruct` | `Document.references` |

**TEI structural variations observed in real files (1,961 .tei files under /data/):**

1. **Small papers (~22KB):** Simple author list, 5 body sections with headings, figures with `<figDesc>` and `<graphic>`, footnotes, no funder/acknowledgments
2. **Medium papers (~25KB):** Has `<funder>`, `<keywords>`, acknowledgments section, funding `<listOrg>`, varied bibliography structures including `<editor>`, `<meeting>`, `<publisher>`
3. **Large papers (~95KB):** Has `<date type="published">`, DOI in sourceDesc, `<figure type="table">` elements, annex sections (author contributions, abbreviations), references with DOI and `<ptr>` links
4. **All files:** GROBID v0.8.0, same root element structure, same namespace

**Edge cases to handle:**
- Empty/missing `<title>` (skip)
- Empty/missing `<abstract>` (skip)
- No `<head>` in `<div>` (section without heading — emit paragraphs without heading)
- Malformed bibliography entries (e.g., GROBID parsing artifacts like `<surname>Fig</surname>`)
- Mixed content in paragraphs (text + `<ref>` + text)
- `<s>` sentence elements within `<p>` (when GROBID segment_sentences is used)

### JATS Parser (`jats_parser.py`)

Uses `lxml.etree` for PMC nXML files following the JATS (Journal Article Tag Suite) DTD.

**Key notes:**
- nXML files are stored with `file_class="nXML"` and `file_extension="nxml"`
- No existing full-text parsing code exists in the codebase (only abstract extraction via `pubmed_parser`)
- JATS namespace handling may vary (some files use default namespace, some don't)

**Element mapping:**

| JATS XPath | Document field |
|---|---|
| `//front/article-meta/title-group/article-title` | `Document.title` |
| `//front/article-meta/contrib-group/contrib[@contrib-type="author"]` | `Document.authors` |
| `//front/article-meta/abstract` | `Document.abstract` |
| `//front/article-meta/kwd-group/kwd` | `Document.keywords` |
| `//front/article-meta/article-id[@pub-id-type="doi"]` | `Document.doi` |
| `//body/sec` | `Document.sections` (recursive, uses `<title>` for heading) |
| `//body//p` | `Section.paragraphs` |
| `//body//fig` | `Figure` with `<label>`, `<caption>` |
| `//body//table-wrap` | `Table` with `<label>`, `<caption>`, `<table>/<thead>/<tbody>/<tr>/<th>/<td>` |
| `//body//disp-formula` | `Formula` |
| `//body//list/list-item` | `ListBlock` |
| `//back/ack` | `Document.acknowledgments` |
| `//back/ref-list/ref` | `Document.references` |
| `//back/app-group` | `Document.back_matter` (appendices) |

### Markdown Emitter (`md_emitter.py`)

Produces docling-style Markdown from the `Document` model. Key design principles:

- Output matches docling format for LLM merge compatibility
- Clean, readable Markdown suitable for both AI consumption and human reading
- No YAML frontmatter (docling doesn't use it) — metadata as header text
- Heading levels: `#` for title, `##` for top-level sections, `###` for subsections, etc.
- Tables rendered as GitHub-flavored Markdown tables
- Figures as bold label + caption text
- References as numbered list

**Output format:**

```markdown
# Paper Title

Authors: First Author^1, Second Author^2

^1 Department, Institution
^2 Department, Institution

## Abstract

Abstract paragraph text here...

## 1 Introduction

Body text with citations [1]...

### 1.1 Subsection Heading

More text here...

| Column1 | Column2 | Column3 |
|---------|---------|---------|
| cell1   | cell2   | cell3   |

**Table 1.** Table caption text.

**Figure 1.** Figure caption describing the image.

- Bullet list item 1
- Bullet list item 2

> Formula: E = mc^2

## Acknowledgments

Acknowledgment text...

## References

1. Author A, Author B (2024). Article title. *Journal Name*, 1(2), 3-4. doi:10.1234/example
2. Author C (2023). Another title. *Other Journal*, 5, 100-110.
```

### Converter (`converter.py`)

Main entry point that orchestrates the conversion:

```python
def detect_format(xml_content: bytes) -> str:
    """Detect whether XML is TEI or JATS based on root element/namespace."""
    # TEI: root element is <TEI> with xmlns="http://www.tei-c.org/ns/1.0"
    # JATS: root element is <article> with JATS DTD or NLM namespace
    ...

def convert_xml_to_markdown(xml_content: bytes, source_format: str = "auto") -> str:
    """Convert TEI or JATS XML to Markdown string."""
    if source_format == "auto":
        source_format = detect_format(xml_content)

    if source_format == "tei":
        document = parse_tei(xml_content)
    elif source_format == "jats":
        document = parse_jats(xml_content)
    else:
        raise ValueError(f"Unknown format: {source_format}")

    return emit_markdown(document)
```

## API Endpoint

Add to the existing referencefile router (`api/routers/referencefile_router.py`):

```python
@router.post('/convert_to_md/{curie_or_reference_id}',
             status_code=status.HTTP_201_CREATED,
             response_model=str)
def convert_to_md(curie_or_reference_id: str,
                  mod_abbreviation: str,
                  source_format: str = "auto",       # "auto", "tei", "nxml"
                  source_file_class: str = None,      # target specific source file
                  user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
                  db: Session = db_session):
    """
    Convert a TEI or nXML file to Markdown and store it as a new referencefile.

    Auto-detection: if source_format="auto", checks available files for the reference.
    Prefers nXML over TEI when both exist (publisher-provided, typically higher quality).

    Returns the new referencefile_id on success.
    """
```

**Logic:**
1. Look up available referencefiles for the reference (filter by `source_file_class` if provided)
2. Auto-detect: prefer `nXML` over `tei` (unless `source_format` is explicitly set)
3. Download source file from S3
4. Convert using `convert_xml_to_markdown()`
5. Determine `file_class`:
   - TEI source + main PDF origin → `converted_grobid_main`
   - TEI source + supplement origin → `converted_grobid_supplement`
   - nXML source → `converted_nxml_main` / `converted_nxml_supplement` (pending SCRUM-5799)
6. Upload Markdown as new referencefile with `file_extension="md"`
7. Return referencefile_id

**Error responses:**
- `404`: No convertible source file found for this reference
- `409`: Markdown already exists (unless `upload_if_already_converted=True`)
- `422`: Conversion failed (malformed XML, empty content)
- `500`: Unexpected error

## Batch Script

File: `lit_processing/xml2md/xml2md_batch.py`

Follows the exact pattern of `lit_processing/pdf2tei/pdf2tei.py`:

```python
def main():
    engine = create_engine(SQLALCHEMY_DATABASE_URL, ...)
    db = sessionmaker(bind=engine)()

    # Load jobs from workflow tags (reuse ATP:0000161 children per SCRUM-5777)
    all_jobs = []
    while jobs := get_jobs(db, "text_convert_job", limit, offset):
        all_jobs.extend(jobs)
        offset += limit

    for job in all_jobs:
        ref_id = job['reference_id']
        mod_abbreviation = ...
        reference_curie = job['reference_curie']

        # Find best source file: prefer nXML, fallback to TEI
        source_file = find_best_source_file(db, ref_id, mod_abbreviation)
        if not source_file:
            job_change_atp_code(db, reference_workflow_tag_id, "on_failed")
            continue

        # Download and convert
        file_content = download_file(db, source_file.referencefile_id, ...)
        try:
            markdown = convert_xml_to_markdown(file_content)
        except Exception:
            job_change_atp_code(db, reference_workflow_tag_id, "on_failed")
            continue

        # Upload result
        file_class = determine_file_class(source_file)
        metadata = {
            "reference_curie": reference_curie,
            "display_name": source_file.display_name,
            "file_class": file_class,
            "file_publication_status": "final",
            "file_extension": "md",
            "mod_abbreviation": mod_abbreviation
        }
        file_upload(db, metadata, UploadFile(file=BytesIO(markdown.encode()), ...),
                    upload_if_already_converted=True)
        job_change_atp_code(db, reference_workflow_tag_id, "on_success")

    # Send error report
    if objects_with_errors:
        send_report("xml2md conversion errors", error_message)
```

**Cron schedule:** Add to `crontab` alongside pdf2tei (e.g., weekly on a different day).

## TEI XML Structure Reference

All TEI files in the system are GROBID v0.8.0 output. Common structure:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<TEI xml:space="preserve" xmlns="http://www.tei-c.org/ns/1.0"
     xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
     xsi:schemaLocation="http://www.tei-c.org/ns/1.0 ..."
     xmlns:xlink="http://www.w3.org/1999/xlink">
  <teiHeader xml:lang="en">
    <fileDesc>
      <titleStmt>
        <title level="a" type="main">Paper Title</title>
        <funder>...</funder>                              <!-- OPTIONAL -->
      </titleStmt>
      <publicationStmt>
        <publisher/>
        <availability status="unknown"><licence/></availability>
        <date type="published" when="2024-01-01"/>        <!-- OPTIONAL -->
      </publicationStmt>
      <sourceDesc>
        <biblStruct>
          <analytic>
            <author>
              <persName><forename>First</forename><surname>Last</surname></persName>
              <email>author@example.com</email>
              <affiliation>
                <orgName type="department">Dept</orgName>
                <orgName type="institution">University</orgName>
                <address><country>Country</country></address>
              </affiliation>
            </author>
            <title level="a" type="main">Paper Title</title>
          </analytic>
          <monogr><imprint><date when="2024"/></imprint></monogr>
          <idno type="MD5">...</idno>
          <idno type="DOI">10.1234/example</idno>         <!-- OPTIONAL -->
          <note type="submission">Received date...</note>  <!-- OPTIONAL -->
        </biblStruct>
      </sourceDesc>
    </fileDesc>
    <encodingDesc>
      <appInfo>
        <application version="0.8.0" ident="GROBID" when="2024-12-06T20:05+0000">
          <desc>GROBID - A machine learning software...</desc>
        </application>
      </appInfo>
    </encodingDesc>
    <profileDesc>
      <textClass>                                          <!-- OPTIONAL -->
        <keywords><term>keyword1</term><term>keyword2</term></keywords>
      </textClass>
      <abstract>
        <div xmlns="http://www.tei-c.org/ns/1.0">
          <p>Abstract text...</p>
        </div>
      </abstract>
    </profileDesc>
  </teiHeader>
  <text xml:lang="en">
    <body>
      <div xmlns="http://www.tei-c.org/ns/1.0">
        <head n="1">Introduction</head>
        <p>Body text with <ref type="bibr" target="#b0">[1]</ref> citations...</p>
        <figure xml:id="fig_0">
          <head>Figure 1.</head>
          <label>1</label>
          <figDesc>Caption describing the figure.</figDesc>
          <graphic coords="..." url="image.png"/>
        </figure>
        <figure type="table" xml:id="tab_0">
          <head>Table 1.</head>
          <label>Table 1</label>
          <figDesc>Table caption.</figDesc>
          <table>
            <row><cell>Header1</cell><cell>Header2</cell></row>
            <row><cell>Val1</cell><cell>Val2</cell></row>
          </table>
        </figure>
        <formula xml:id="formula_0">E = mc^2 (1)</formula>
        <list>
          <item>List item 1</item>
          <item>List item 2</item>
        </list>
        <note place="foot" n="1">Footnote text.</note>
      </div>
      <div xmlns="http://www.tei-c.org/ns/1.0">
        <head n="2">Methods</head>
        <p>More text...</p>
        <div>
          <head n="2.1">Subsection</head>
          <p>Nested section text...</p>
        </div>
      </div>
    </body>
    <back>
      <div type="acknowledgement">                         <!-- OPTIONAL -->
        <div><head>Acknowledgements</head><p>Thanks to...</p></div>
      </div>
      <listOrg type="funding">...</listOrg>                <!-- OPTIONAL -->
      <div type="annex">                                   <!-- OPTIONAL -->
        <div><head>Author Contributions</head><p>...</p></div>
        <div><head>Abbreviations</head><p>...</p></div>
      </div>
      <div type="references">
        <listBibl>
          <biblStruct xml:id="b0">
            <analytic>
              <title level="a">Reference title</title>
              <author><persName><forename>A</forename><surname>Author</surname></persName></author>
            </analytic>
            <monogr>
              <title level="j">Journal Name</title>
              <imprint>
                <biblScope unit="volume">1</biblScope>
                <biblScope unit="issue">2</biblScope>
                <biblScope unit="page" from="3" to="4"/>
                <date type="published" when="2024"/>
              </imprint>
            </monogr>
            <idno type="DOI">10.1234/ref</idno>           <!-- OPTIONAL -->
          </biblStruct>
        </listBibl>
      </div>
    </back>
  </text>
</TEI>
```

## JATS nXML Structure Reference

PMC nXML files follow the JATS DTD. Typical structure:

```xml
<!DOCTYPE article PUBLIC "-//NLM//DTD JATS ...//EN" "...">
<article article-type="research-article">
  <front>
    <journal-meta>
      <journal-id journal-id-type="nlm-ta">J Name</journal-id>
      <journal-title-group><journal-title>Journal Name</journal-title></journal-title-group>
      <issn pub-type="ppub">1234-5678</issn>
    </journal-meta>
    <article-meta>
      <article-id pub-id-type="pmid">12345678</article-id>
      <article-id pub-id-type="doi">10.1234/example</article-id>
      <article-id pub-id-type="pmc">PMC1234567</article-id>
      <title-group>
        <article-title>Paper Title</article-title>
      </title-group>
      <contrib-group>
        <contrib contrib-type="author">
          <name><surname>Last</surname><given-names>First M</given-names></name>
          <email>author@example.com</email>
          <xref ref-type="aff" rid="aff1"/>
        </contrib>
      </contrib-group>
      <aff id="aff1">Department, Institution, City, Country</aff>
      <pub-date pub-type="epub"><year>2024</year></pub-date>
      <volume>1</volume><issue>2</issue><fpage>3</fpage><lpage>4</lpage>
      <kwd-group><kwd>keyword1</kwd><kwd>keyword2</kwd></kwd-group>
      <abstract><p>Abstract text...</p></abstract>
    </article-meta>
  </front>
  <body>
    <sec id="sec1">
      <title>Introduction</title>
      <p>Body text with <xref ref-type="bibr" rid="ref1">[1]</xref> citations...</p>
      <fig id="fig1">
        <label>Figure 1</label>
        <caption><title>Figure title</title><p>Caption text.</p></caption>
        <graphic xlink:href="image.tif"/>
      </fig>
      <table-wrap id="tab1">
        <label>Table 1</label>
        <caption><title>Table title</title><p>Caption text.</p></caption>
        <table>
          <thead><tr><th>Header1</th><th>Header2</th></tr></thead>
          <tbody><tr><td>Val1</td><td>Val2</td></tr></tbody>
        </table>
      </table-wrap>
      <sec id="sec1.1">
        <title>Subsection</title>
        <p>Nested content...</p>
      </sec>
    </sec>
  </body>
  <back>
    <ack><p>Acknowledgment text...</p></ack>
    <ref-list>
      <ref id="ref1">
        <element-citation publication-type="journal">
          <person-group person-group-type="author">
            <name><surname>Author</surname><given-names>A</given-names></name>
          </person-group>
          <article-title>Reference title</article-title>
          <source>Journal Name</source>
          <year>2024</year>
          <volume>1</volume><issue>2</issue>
          <fpage>3</fpage><lpage>4</lpage>
          <pub-id pub-id-type="doi">10.1234/ref</pub-id>
          <pub-id pub-id-type="pmid">87654321</pub-id>
        </element-citation>
      </ref>
    </ref-list>
    <app-group>
      <app><title>Appendix</title><p>Supplementary content...</p></app>
    </app-group>
  </back>
</article>
```

## Existing Code Reference

### Files that currently consume TEI (in agr_automated_information_extraction)

| File | What it does |
|---|---|
| `utils/tei_utils.py` | Central TEI parsing — `AllianceTEI` class with `get_title()`, `get_abstract()`, `get_fulltext()`, `get_sentences()` |
| `utils/get_documents.py` | Loads `.tei` files via `grobid_client.types.TEI.parse()`, extracts sections/abstract/title |
| `utils/abc_utils.py` | Downloads TEI files from ABC API (`download_tei_files_for_references`) |
| `agr_dataset_manager/dataset_downloader.py` | Downloads TEI files, falls back to PDF-to-TEI via GROBID |
| `agr_entity_extractor/agr_entity_extraction_pipeline*.py` | Uses `AllianceTEI` to extract title/abstract/fulltext for NER |

### Existing pdf2tei pattern (in agr_literature_service)

The `lit_processing/pdf2tei/pdf2tei.py` file is the template for the batch script:
- Gets jobs via `get_jobs(db, "text_convert_job", limit, offset)`
- Downloads source file via `download_file(db, referencefile_id, ...)`
- Converts content
- Uploads result via `file_upload(db, metadata, UploadFile(...))`
- Transitions workflow via `job_change_atp_code(db, reference_workflow_tag_id, "on_success"/"on_failed")`
- Sends error report via `send_report(subject, error_message)`

### Workflow ATP IDs (per SCRUM-5777 discussion)

- `ATP:0000161` — text conversion process (parent)
- `ATP:0000162` — text conversion pending / in queue
- `ATP:0000163` — file converted to text (success)
- `ATP:0000164` — text conversion failed

The xml2md batch script should reuse these same ATP IDs.

## Dependencies

No new dependencies needed. The converter uses:
- `lxml` (already installed — used by pdf2tei, email extraction, etc.)
- `dataclasses` (stdlib)
- Standard `agr_literature_service` imports (crud, models, schemas)

## Testing Strategy

1. **Unit tests for each parser:** Feed sample TEI/JATS XML strings, verify `Document` model output
2. **Unit tests for Markdown emitter:** Feed `Document` models, verify Markdown string output
3. **Integration tests:** Full XML-to-Markdown pipeline with real TEI files from `/data/`
4. **API endpoint tests:** Mock S3/DB, verify HTTP responses and file storage
5. **Edge case tests:** Empty title, missing abstract, no sections, malformed XML, etc.

Test fixtures:
- Use the existing test TEI from `tests/lit_processing/pdf2tei/test_pdf2tei.py` (lines 20-85)
- Create comprehensive TEI and JATS fixtures covering all element types
- Use 2-3 real .tei files from `/data/agr_document_classifier/training/` for integration tests

## Implementation Order

1. `models.py` — data model (no dependencies)
2. `tei_parser.py` — TEI parsing with unit tests
3. `jats_parser.py` — JATS parsing with unit tests
4. `md_emitter.py` — Markdown generation with unit tests
5. `converter.py` — orchestration with format detection
6. API endpoint addition to referencefile router
7. `xml2md_batch.py` — batch processing script
8. Integration tests with real files
9. Crontab entry for batch processing
