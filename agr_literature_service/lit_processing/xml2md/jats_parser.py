"""Parse PMC nXML/JATS XML into the intermediate Document model."""
from __future__ import annotations

from lxml import etree

from agr_literature_service.lit_processing.xml2md.models import (
    Author, Document, Figure, Formula, InlineRef, ListBlock,
    Paragraph, Reference, Section, Table, TableCell,
)
from agr_literature_service.lit_processing.xml2md.xml_utils import (
    all_text, parse_xml, text,
)


def parse_jats(
    xml_content: bytes,
    root: etree._Element | None = None,
) -> Document:
    """Parse JATS/nXML content into a Document model.

    Handles both namespaced and non-namespaced JATS files.

    Args:
        xml_content: Raw bytes of a JATS XML file.
        root: Optional pre-parsed XML root element. When provided,
            *xml_content* is ignored and no re-parsing occurs.

    Returns:
        A populated Document dataclass.
    """
    if root is None:
        root = parse_xml(xml_content)
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


def _parse_title(root: etree._Element) -> str:
    """Extract title from article-meta/title-group/article-title."""
    title_elem = root.find(".//article-meta/title-group/article-title")
    return all_text(title_elem)


def _parse_doi(root: etree._Element) -> str:
    """Extract DOI from article-id[@pub-id-type='doi']."""
    doi_elem = root.find(".//article-meta/article-id[@pub-id-type='doi']")
    return text(doi_elem)


def _parse_keywords(root: etree._Element) -> list[str]:
    """Extract keywords from kwd-group/kwd."""
    keywords: list[str] = []
    for kwd in root.findall(".//article-meta/kwd-group/kwd"):
        kwd_text = all_text(kwd)
        if kwd_text:
            keywords.append(kwd_text)
    return keywords


def _parse_abstract(root: etree._Element) -> list[Paragraph]:
    """Extract abstract paragraphs.

    Handles both plain abstracts (<abstract><p>...) and structured
    abstracts (<abstract><sec><title>Background</title><p>...) common
    in PMC papers.
    """
    abstract_elem = root.find(".//article-meta/abstract")
    if abstract_elem is None:
        return []

    paragraphs: list[Paragraph] = []

    # Check for structured abstract with <sec> children
    secs = abstract_elem.findall("sec")
    if secs:
        for sec in secs:
            title_elem = sec.find("title")
            sec_title = all_text(title_elem) if title_elem is not None else ""
            for p_elem in sec.findall("p"):
                para = _parse_paragraph(p_elem)
                if para.text and sec_title:
                    para.text = f"**{sec_title}:** {para.text}"
                if para.text:
                    paragraphs.append(para)
    else:
        for p_elem in abstract_elem.findall(".//p"):
            para = _parse_paragraph(p_elem)
            if para.text:
                paragraphs.append(para)

    return paragraphs


def _parse_affiliations(root: etree._Element) -> dict[str, str]:
    """Build a map of aff id -> affiliation text."""
    aff_map: dict[str, str] = {}
    for aff_elem in root.findall(".//article-meta/aff"):
        aff_id = aff_elem.get("id", "")
        aff_text = all_text(aff_elem)
        if aff_id and aff_text:
            aff_map[aff_id] = aff_text
    return aff_map


def _parse_authors(root: etree._Element, aff_map: dict[str, str]) -> list[Author]:
    """Extract authors from contrib-group, resolving affiliations via xref."""
    authors = []
    for contrib in root.findall(
        ".//article-meta/contrib-group/contrib[@contrib-type='author']"
    ):
        author = Author()
        name_elem = contrib.find("name")
        if name_elem is not None:
            author.surname = text(name_elem.find("surname"))
            author.given_name = text(name_elem.find("given-names"))

        email_elem = contrib.find("email")
        if email_elem is not None:
            author.email = text(email_elem)

        # Resolve affiliations via xref
        for xref in contrib.findall("xref[@ref-type='aff']"):
            rid = xref.get("rid", "")
            if rid in aff_map:
                author.affiliations.append(aff_map[rid])

        authors.append(author)
    return authors


def _parse_body(root: etree._Element) -> list[Section]:
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
                    or preamble.tables or preamble.lists
                    or preamble.formulas):
                sections.append(preamble)
                preamble = Section(level=1)
            sections.append(_parse_sec(child, level=1))
        elif tag == "p":
            _collect_from_p(child, preamble)
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
            or preamble.tables or preamble.lists
            or preamble.formulas):
        sections.append(preamble)

    return sections


_BLOCK_TAGS = frozenset({"fig", "table-wrap", "disp-formula", "list"})


def _parse_sec(sec_elem: etree._Element, level: int) -> Section:
    """Recursively parse a <sec> element into a Section."""
    section = Section(level=level)

    title_elem = sec_elem.find("title")
    if title_elem is not None:
        section.heading = all_text(title_elem)

    for child in sec_elem:
        tag = etree.QName(child.tag).localname if isinstance(
            child.tag, str
        ) else ""

        if tag == "title":
            continue
        elif tag == "p":
            _collect_from_p(child, section)
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


def _collect_from_p(p_elem: etree._Element, section: Section) -> None:
    """Parse a <p> element, extracting any embedded block elements.

    JATS allows block-level elements (<table-wrap>, <fig>, <disp-formula>,
    <list>) to be nested inside <p>.  When that happens the paragraph text
    before/after the block element is emitted as separate paragraphs, and
    the block element is added to the appropriate section list.
    """
    child_tags = {
        etree.QName(c.tag).localname
        for c in p_elem
        if isinstance(c.tag, str)
    }
    if not child_tags & _BLOCK_TAGS:
        # Fast path — no embedded blocks, just parse normally
        section.paragraphs.append(_parse_paragraph(p_elem))
        return

    # Slow path — split around embedded block elements
    for child in p_elem:
        tag = etree.QName(child.tag).localname if isinstance(
            child.tag, str
        ) else ""
        if tag == "fig":
            section.figures.append(_parse_fig(child))
        elif tag == "table-wrap":
            section.tables.append(_parse_table_wrap(child))
        elif tag == "disp-formula":
            section.formulas.append(_parse_formula(child))
        elif tag == "list":
            section.lists.append(_parse_list(child))

    # Emit the paragraph text (with block elements excluded)
    para = _parse_paragraph(p_elem, skip_tags=_BLOCK_TAGS)
    if para.text:
        section.paragraphs.append(para)


def _parse_paragraph(
    p_elem: etree._Element,
    skip_tags: frozenset[str] | None = None,
) -> Paragraph:
    """Parse a <p> element with mixed content, extracting inline refs.

    Args:
        p_elem: The ``<p>`` XML element.
        skip_tags: Optional set of child tag localnames to skip (used when
            block elements like ``<table-wrap>`` are extracted separately).
    """
    parts: list[str] = []
    refs: list[InlineRef] = []

    if p_elem.text:
        parts.append(p_elem.text)

    for child in p_elem:
        tag = etree.QName(child.tag).localname if isinstance(
            child.tag, str
        ) else ""

        if skip_tags and tag in skip_tags:
            # Still collect the tail text after the skipped element
            if child.tail:
                parts.append(child.tail)
            continue

        if tag == "xref":
            ref_text = all_text(child)
            rid = child.get("rid", "")
            if ref_text:
                refs.append(InlineRef(text=ref_text, target=rid))
                parts.append(ref_text)
        else:
            parts.append(all_text(child))

        if child.tail:
            parts.append(child.tail)

    para_text = "".join(parts).strip()
    return Paragraph(text=para_text, refs=refs)


def _parse_fig(fig_elem: etree._Element) -> Figure:
    """Parse a <fig> element."""
    fig = Figure()

    label_elem = fig_elem.find("label")
    if label_elem is not None:
        fig.label = all_text(label_elem)

    caption_elem = fig_elem.find("caption")
    if caption_elem is not None:
        # Caption may have <title> and/or <p>
        parts = []
        title = caption_elem.find("title")
        if title is not None:
            parts.append(all_text(title))
        for p in caption_elem.findall("p"):
            parts.append(all_text(p))
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


def _parse_table_wrap(tw_elem: etree._Element) -> Table:
    """Parse a <table-wrap> element."""
    table = Table()

    label_elem = tw_elem.find("label")
    if label_elem is not None:
        table.label = all_text(label_elem)

    caption_elem = tw_elem.find("caption")
    if caption_elem is not None:
        parts = []
        title = caption_elem.find("title")
        if title is not None:
            parts.append(all_text(title))
        for p in caption_elem.findall("p"):
            parts.append(all_text(p))
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


def _parse_table_row(tr_elem: etree._Element, is_header: bool) -> list[TableCell]:
    """Parse a <tr> element into a list of TableCells.

    Handles colspan by emitting empty padding cells so GFM tables
    stay aligned.
    """
    cells = []
    for child in tr_elem:
        tag = etree.QName(child.tag).localname if isinstance(
            child.tag, str
        ) else ""
        if tag in ("th", "td"):
            colspan = int(child.get("colspan", "1") or "1")
            cell = TableCell(
                text=all_text(child),
                is_header=(tag == "th" or is_header),
            )
            cells.append(cell)
            for _ in range(colspan - 1):
                cells.append(TableCell(
                    text="",
                    is_header=(tag == "th" or is_header),
                ))
    return cells


def _parse_formula(formula_elem: etree._Element) -> Formula:
    """Parse a <disp-formula> element."""
    label_elem = formula_elem.find("label")
    label = all_text(label_elem)

    formula_text = all_text(formula_elem)
    # Remove label from text — may appear at start or end
    if label:
        if formula_text.endswith(label):
            formula_text = formula_text[:-len(label)].strip()
        elif formula_text.startswith(label):
            formula_text = formula_text[len(label):].strip()

    return Formula(text=formula_text, label=label)


def _parse_list(list_elem: etree._Element) -> ListBlock:
    """Parse a <list> element."""
    list_type = list_elem.get("list-type", "")
    ordered = list_type in ("order", "ordered", "number")

    items: list[str] = []
    for item_elem in list_elem.findall("list-item"):
        # list-item typically contains <p>
        p = item_elem.find("p")
        if p is not None:
            item_text = all_text(p)
        else:
            item_text = all_text(item_elem)
        if item_text:
            items.append(item_text)

    return ListBlock(items=items, ordered=ordered)


def _parse_acknowledgments(root: etree._Element) -> str:
    """Extract acknowledgments from //back/ack."""
    ack = root.find(".//back/ack")
    if ack is None:
        return ""

    parts: list[str] = []
    for p in ack.findall(".//p"):
        p_text = all_text(p)
        if p_text:
            parts.append(p_text)
    return "\n\n".join(parts)


def _parse_appendices(root: etree._Element) -> list[Section]:
    """Extract appendices from //back/app-group."""
    sections: list[Section] = []
    app_group = root.find(".//back/app-group")
    if app_group is None:
        return sections

    for app in app_group.findall("app"):
        section = Section(level=1)
        title_elem = app.find("title")
        if title_elem is not None:
            section.heading = all_text(title_elem)
        for p in app.findall("p"):
            section.paragraphs.append(Paragraph(text=all_text(p)))
        sections.append(section)

    return sections


def _parse_floats_group(root: etree._Element, doc: Document) -> None:
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


def _parse_bibliography(root: etree._Element) -> list[Reference]:
    """Extract references from //back/ref-list/ref."""
    references = []
    for idx, ref_elem in enumerate(
        root.findall(".//back/ref-list/ref")
    ):
        ref = _parse_ref(ref_elem, idx + 1)
        references.append(ref)
    return references


def _parse_ref(ref_elem: etree._Element, index: int) -> Reference:
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
        surname = text(name_elem.find("surname"))
        given = text(name_elem.find("given-names"))
        if surname:
            if given:
                ref.authors.append(f"{surname} {given}")
            else:
                ref.authors.append(surname)

    # Title
    title_elem = citation.find("article-title")
    if title_elem is not None:
        ref.title = all_text(title_elem)

    # Journal
    source_elem = citation.find("source")
    if source_elem is not None:
        ref.journal = all_text(source_elem)

    # Year
    year_elem = citation.find("year")
    if year_elem is not None:
        ref.year = text(year_elem)

    # Volume
    volume_elem = citation.find("volume")
    if volume_elem is not None:
        ref.volume = text(volume_elem)

    # Issue
    issue_elem = citation.find("issue")
    if issue_elem is not None:
        ref.issue = text(issue_elem)

    # Pages
    fpage = citation.find("fpage")
    lpage = citation.find("lpage")
    if fpage is not None:
        fpage_text = text(fpage)
        lpage_text = text(lpage) if lpage is not None else ""
        if fpage_text and lpage_text:
            ref.pages = f"{fpage_text}-{lpage_text}"
        elif fpage_text:
            ref.pages = fpage_text

    # DOI
    doi_elem = citation.find("pub-id[@pub-id-type='doi']")
    if doi_elem is not None:
        ref.doi = text(doi_elem)

    # PMID
    pmid_elem = citation.find("pub-id[@pub-id-type='pmid']")
    if pmid_elem is not None:
        ref.pmid = text(pmid_elem)

    return ref
