"""Parse PMC nXML/JATS XML into the intermediate Document model."""
from __future__ import annotations

from lxml import etree

from agr_literature_service.lit_processing.xml2md.models import (
    Author, Document, Figure, Formula, ListBlock,
    Paragraph, Reference, Section, Table, TableCell,
)


def parse_jats(xml_content: bytes) -> Document:
    """Parse JATS/nXML content into a Document model.

    Handles both namespaced and non-namespaced JATS files.

    Args:
        xml_content: Raw bytes of a JATS XML file.

    Returns:
        A populated Document dataclass.
    """
    # Strip DOCTYPE to avoid DTD resolution issues
    parser = etree.XMLParser(
        recover=True, no_network=True, load_dtd=False, resolve_entities=False
    )
    root = etree.fromstring(xml_content, parser=parser)
    doc = Document(source_format="jats")

    doc.title = _parse_title(root)
    doc.doi = _parse_doi(root)
    doc.keywords = _parse_keywords(root)
    doc.abstract = _parse_abstract(root)

    # Parse affiliations first, then resolve into authors
    aff_map = _parse_affiliations(root)
    doc.authors = _parse_authors(root, aff_map)

    doc.sections = _parse_body(root)
    doc.references = _parse_bibliography(root)
    doc.acknowledgments = _parse_acknowledgments(root)
    doc.back_matter = _parse_appendices(root)

    # Parse floats-group (figures/tables outside body, common in PMC nXML)
    _parse_floats_group(root, doc)

    return doc


def _find(elem, xpath: str):
    """Find element using XPath, works without namespaces."""
    return elem.find(xpath)


def _findall(elem, xpath: str):
    """Find all elements using XPath, works without namespaces."""
    return elem.findall(xpath)


def _text(elem) -> str:
    """Get stripped text content of an element, or empty string."""
    if elem is None or elem.text is None:
        return ""
    return elem.text.strip()


def _all_text(elem) -> str:
    """Get all text content of an element including children."""
    return _collect_text(elem).strip()


def _collect_text(elem) -> str:
    """Recursively collect text without stripping inner whitespace."""
    if elem is None:
        return ""
    parts = []
    if elem.text:
        parts.append(elem.text)
    for child in elem:
        parts.append(_collect_text(child))
        if child.tail:
            parts.append(child.tail)
    return "".join(parts)


def _parse_title(root) -> str:
    """Extract title from article-meta/title-group/article-title."""
    title_elem = root.find(".//article-meta/title-group/article-title")
    return _all_text(title_elem)


def _parse_doi(root) -> str:
    """Extract DOI from article-id[@pub-id-type='doi']."""
    doi_elem = root.find(".//article-meta/article-id[@pub-id-type='doi']")
    return _text(doi_elem)


def _parse_keywords(root) -> list[str]:
    """Extract keywords from kwd-group/kwd."""
    keywords = []
    for kwd in root.findall(".//article-meta/kwd-group/kwd"):
        text = _all_text(kwd)
        if text:
            keywords.append(text)
    return keywords


def _parse_abstract(root) -> list[Paragraph]:
    """Extract abstract paragraphs."""
    abstract_elem = root.find(".//article-meta/abstract")
    if abstract_elem is None:
        return []

    paragraphs = []
    for p_elem in abstract_elem.findall(".//p"):
        para = _parse_paragraph(p_elem)
        if para.text:
            paragraphs.append(para)
    return paragraphs


def _parse_affiliations(root) -> dict[str, str]:
    """Build a map of aff id -> affiliation text."""
    aff_map: dict[str, str] = {}
    for aff_elem in root.findall(".//article-meta/aff"):
        aff_id = aff_elem.get("id", "")
        aff_text = _all_text(aff_elem)
        if aff_id and aff_text:
            aff_map[aff_id] = aff_text
    return aff_map


def _parse_authors(root, aff_map: dict[str, str]) -> list[Author]:
    """Extract authors from contrib-group, resolving affiliations via xref."""
    authors = []
    for contrib in root.findall(
        ".//article-meta/contrib-group/contrib[@contrib-type='author']"
    ):
        author = Author()
        name_elem = contrib.find("name")
        if name_elem is not None:
            author.surname = _text(name_elem.find("surname"))
            author.given_name = _text(name_elem.find("given-names"))

        email_elem = contrib.find("email")
        if email_elem is not None:
            author.email = _text(email_elem)

        # Resolve affiliations via xref
        for xref in contrib.findall("xref[@ref-type='aff']"):
            rid = xref.get("rid", "")
            if rid in aff_map:
                author.affiliations.append(aff_map[rid])

        authors.append(author)
    return authors


def _parse_body(root) -> list[Section]:
    """Parse body content from <body>.

    Handles both <sec>-structured bodies and bare elements (<p>, <fig>,
    <table-wrap>) that appear as direct children of <body>.
    """
    sections: list[Section] = []
    body = root.find(".//body")
    if body is None:
        return sections

    # Collect bare (non-sec) elements before the first <sec> into a
    # preamble section, and handle any bare elements between sections.
    preamble = Section(level=1)

    for child in body:
        tag = etree.QName(child.tag).localname if isinstance(
            child.tag, str
        ) else ""

        if tag == "sec":
            # Flush any accumulated preamble content
            if (preamble.paragraphs or preamble.figures
                    or preamble.tables or preamble.lists):
                sections.append(preamble)
                preamble = Section(level=1)
            sections.append(_parse_sec(child, level=1))
        elif tag == "p":
            preamble.paragraphs.append(_parse_paragraph(child))
        elif tag == "fig":
            preamble.figures.append(_parse_fig(child))
        elif tag == "table-wrap":
            preamble.tables.append(_parse_table_wrap(child))
        elif tag == "disp-formula":
            preamble.formulas.append(_parse_formula(child))
        elif tag == "list":
            preamble.lists.append(_parse_list(child))

    # Flush trailing preamble
    if (preamble.paragraphs or preamble.figures
            or preamble.tables or preamble.lists):
        sections.append(preamble)

    return sections


def _parse_sec(sec_elem, level: int) -> Section:
    """Recursively parse a <sec> element into a Section."""
    section = Section(level=level)

    title_elem = sec_elem.find("title")
    if title_elem is not None:
        section.heading = _all_text(title_elem)

    for child in sec_elem:
        tag = etree.QName(child.tag).localname if isinstance(
            child.tag, str
        ) else ""

        if tag == "title":
            continue
        elif tag == "p":
            section.paragraphs.append(_parse_paragraph(child))
        elif tag == "sec":
            section.subsections.append(_parse_sec(child, level + 1))
        elif tag == "fig":
            section.figures.append(_parse_fig(child))
        elif tag == "table-wrap":
            section.tables.append(_parse_table_wrap(child))
        elif tag == "disp-formula":
            section.formulas.append(_parse_formula(child))
        elif tag == "list":
            section.lists.append(_parse_list(child))

    return section


def _parse_paragraph(p_elem) -> Paragraph:
    """Parse a <p> element with mixed content."""
    text = _all_text(p_elem)
    return Paragraph(text=text)


def _parse_fig(fig_elem) -> Figure:
    """Parse a <fig> element."""
    fig = Figure()

    label_elem = fig_elem.find("label")
    if label_elem is not None:
        fig.label = _all_text(label_elem)

    caption_elem = fig_elem.find("caption")
    if caption_elem is not None:
        # Caption may have <title> and/or <p>
        parts = []
        title = caption_elem.find("title")
        if title is not None:
            parts.append(_all_text(title))
        for p in caption_elem.findall("p"):
            parts.append(_all_text(p))
        fig.caption = " ".join(parts)

    graphic_elem = fig_elem.find("graphic")
    if graphic_elem is not None:
        # xlink:href attribute — try with and without namespace
        href = graphic_elem.get(
            "{http://www.w3.org/1999/xlink}href", ""
        )
        if not href:
            href = graphic_elem.get("href", "")
        fig.graphic_url = href

    return fig


def _parse_table_wrap(tw_elem) -> Table:
    """Parse a <table-wrap> element."""
    table = Table()

    label_elem = tw_elem.find("label")
    if label_elem is not None:
        table.label = _all_text(label_elem)

    caption_elem = tw_elem.find("caption")
    if caption_elem is not None:
        parts = []
        title = caption_elem.find("title")
        if title is not None:
            parts.append(_all_text(title))
        for p in caption_elem.findall("p"):
            parts.append(_all_text(p))
        table.caption = " ".join(parts)

    table_elem = tw_elem.find("table")
    if table_elem is not None:
        # Parse thead
        thead = table_elem.find("thead")
        if thead is not None:
            for tr in thead.findall("tr"):
                row = _parse_table_row(tr, is_header=True)
                if row:
                    table.rows.append(row)

        # Parse tbody
        tbody = table_elem.find("tbody")
        if tbody is not None:
            for tr in tbody.findall("tr"):
                row = _parse_table_row(tr, is_header=False)
                if row:
                    table.rows.append(row)

        # Direct tr elements (no thead/tbody)
        if thead is None and tbody is None:
            for tr in table_elem.findall("tr"):
                row = _parse_table_row(tr, is_header=False)
                if row:
                    table.rows.append(row)

    return table


def _parse_table_row(tr_elem, is_header: bool) -> list[TableCell]:
    """Parse a <tr> element into a list of TableCells."""
    cells = []
    for child in tr_elem:
        tag = etree.QName(child.tag).localname if isinstance(
            child.tag, str
        ) else ""
        if tag in ("th", "td"):
            cell = TableCell(
                text=_all_text(child),
                is_header=(tag == "th" or is_header),
            )
            cells.append(cell)
    return cells


def _parse_formula(formula_elem) -> Formula:
    """Parse a <disp-formula> element."""
    label_elem = formula_elem.find("label")
    label = _all_text(label_elem)

    text = _all_text(formula_elem)
    # Remove label from text if present at end
    if label and text.endswith(label):
        text = text[:-len(label)].strip()

    return Formula(text=text, label=label)


def _parse_list(list_elem) -> ListBlock:
    """Parse a <list> element."""
    list_type = list_elem.get("list-type", "")
    ordered = list_type in ("order", "ordered", "number")

    items = []
    for item_elem in list_elem.findall("list-item"):
        # list-item typically contains <p>
        p = item_elem.find("p")
        if p is not None:
            text = _all_text(p)
        else:
            text = _all_text(item_elem)
        if text:
            items.append(text)

    return ListBlock(items=items, ordered=ordered)


def _parse_acknowledgments(root) -> str:
    """Extract acknowledgments from //back/ack."""
    ack = root.find(".//back/ack")
    if ack is None:
        return ""

    parts = []
    for p in ack.findall(".//p"):
        text = _all_text(p)
        if text:
            parts.append(text)
    return "\n\n".join(parts)


def _parse_appendices(root) -> list[Section]:
    """Extract appendices from //back/app-group."""
    sections: list[Section] = []
    app_group = root.find(".//back/app-group")
    if app_group is None:
        return sections

    for app in app_group.findall("app"):
        section = Section(level=1)
        title_elem = app.find("title")
        if title_elem is not None:
            section.heading = _all_text(title_elem)
        for p in app.findall("p"):
            section.paragraphs.append(Paragraph(text=_all_text(p)))
        sections.append(section)

    return sections


def _parse_floats_group(root, doc: Document) -> None:
    """Parse <floats-group> for figures and tables outside body/back.

    In many PMC nXML files, figures and tables are placed in a
    <floats-group> element that is a direct child of <article>,
    separate from <body> and <back>.
    """
    floats = root.find(".//floats-group")
    if floats is None:
        return

    for child in floats:
        tag = etree.QName(child.tag).localname if isinstance(
            child.tag, str
        ) else ""

        if tag == "fig":
            doc.figures.append(_parse_fig(child))
        elif tag == "table-wrap":
            doc.tables.append(_parse_table_wrap(child))


def _parse_bibliography(root) -> list[Reference]:
    """Extract references from //back/ref-list/ref."""
    references = []
    for idx, ref_elem in enumerate(
        root.findall(".//back/ref-list/ref")
    ):
        ref = _parse_ref(ref_elem, idx + 1)
        references.append(ref)
    return references


def _parse_ref(ref_elem, index: int) -> Reference:
    """Parse a single <ref> element.

    Supports both <element-citation> and <mixed-citation>.
    """
    ref = Reference(index=index)

    # Try element-citation first, fall back to mixed-citation
    citation = ref_elem.find("element-citation")
    if citation is None:
        citation = ref_elem.find("mixed-citation")
    if citation is None:
        return ref

    # Authors — try person-group/name first, fall back to direct name children
    author_names = citation.findall(".//person-group/name")
    if not author_names:
        author_names = citation.findall("name")
    for name_elem in author_names:
        surname = _text(name_elem.find("surname"))
        given = _text(name_elem.find("given-names"))
        if surname:
            if given:
                ref.authors.append(f"{surname} {given}")
            else:
                ref.authors.append(surname)

    # Title
    title_elem = citation.find("article-title")
    if title_elem is not None:
        ref.title = _all_text(title_elem)

    # Journal
    source_elem = citation.find("source")
    if source_elem is not None:
        ref.journal = _all_text(source_elem)

    # Year
    year_elem = citation.find("year")
    if year_elem is not None:
        ref.year = _text(year_elem)

    # Volume
    volume_elem = citation.find("volume")
    if volume_elem is not None:
        ref.volume = _text(volume_elem)

    # Issue
    issue_elem = citation.find("issue")
    if issue_elem is not None:
        ref.issue = _text(issue_elem)

    # Pages
    fpage = citation.find("fpage")
    lpage = citation.find("lpage")
    if fpage is not None:
        fpage_text = _text(fpage)
        lpage_text = _text(lpage) if lpage is not None else ""
        if fpage_text and lpage_text:
            ref.pages = f"{fpage_text}-{lpage_text}"
        elif fpage_text:
            ref.pages = fpage_text

    # DOI
    doi_elem = citation.find("pub-id[@pub-id-type='doi']")
    if doi_elem is not None:
        ref.doi = _text(doi_elem)

    # PMID
    pmid_elem = citation.find("pub-id[@pub-id-type='pmid']")
    if pmid_elem is not None:
        ref.pmid = _text(pmid_elem)

    return ref
