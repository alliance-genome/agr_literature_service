"""Parse GROBID TEI XML into the intermediate Document model."""
from __future__ import annotations

from lxml import etree

from agr_literature_service.lit_processing.xml2md.models import (
    Author, Document, Figure, Formula, InlineRef, ListBlock,
    Paragraph, Reference, Section, Table, TableCell,
)
from agr_literature_service.lit_processing.xml2md.xml_utils import (
    all_text, parse_xml, text,
)

NS = {"tei": "http://www.tei-c.org/ns/1.0"}


def parse_tei(
    xml_content: bytes,
    root: etree._Element | None = None,
) -> Document:
    """Parse GROBID TEI XML content into a Document model.

    Args:
        xml_content: Raw bytes of a TEI XML file.
        root: Optional pre-parsed XML root element. When provided,
            *xml_content* is ignored and no re-parsing occurs.

    Returns:
        A populated Document dataclass.
    """
    if root is None:
        root = parse_xml(xml_content)
    doc = Document(source_format="tei")

    doc.title = _parse_title(root)
    doc.authors = _parse_authors(root)
    doc.abstract = _parse_abstract(root)
    doc.keywords = _parse_keywords(root)
    doc.doi = _parse_doi(root)
    doc.sections = _parse_body(root)
    doc.figures, doc.tables = _parse_top_level_figures(root)
    doc.references = _parse_bibliography(root)
    doc.acknowledgments = _parse_acknowledgments(root)

    # Back matter: annex + additional div types (funding, availability)
    back_matter = _parse_annex(root)
    back_matter.extend(_parse_additional_back(root))
    doc.back_matter = back_matter

    return doc


def _parse_title(root: etree._Element) -> str:
    """Extract title from //teiHeader//title[@level='a']."""
    title_elem = root.find(".//tei:teiHeader//tei:title[@level='a']", NS)
    return all_text(title_elem)


def _parse_authors(root: etree._Element) -> list[Author]:
    """Extract authors from //sourceDesc//author."""
    authors = []
    for author_elem in root.findall(
        ".//tei:sourceDesc//tei:author", NS
    ):
        author = Author()
        persname = author_elem.find("tei:persName", NS)
        if persname is not None:
            # Collect all forename parts (first + middle)
            forenames = persname.findall("tei:forename", NS)
            name_parts = [text(fn) for fn in forenames if text(fn)]
            author.given_name = " ".join(name_parts)

            surname = persname.find("tei:surname", NS)
            author.surname = text(surname)

        email_elem = author_elem.find(".//tei:email", NS)
        if email_elem is not None:
            author.email = text(email_elem)

        # ORCID
        orcid_elem = author_elem.find("tei:idno[@type='ORCID']", NS)
        if orcid_elem is not None:
            author.orcid = text(orcid_elem)

        for aff_elem in author_elem.findall("tei:affiliation", NS):
            parts = []
            for orgname in aff_elem.findall("tei:orgName", NS):
                org_text = text(orgname)
                if org_text:
                    parts.append(org_text)
            address = aff_elem.find("tei:address", NS)
            if address is not None:
                country = address.find("tei:country", NS)
                if country is not None and text(country):
                    parts.append(text(country))
            if parts:
                author.affiliations.append(", ".join(parts))

        authors.append(author)
    return authors


def _parse_abstract(root: etree._Element) -> list[Paragraph]:
    """Extract abstract paragraphs.

    Handles both <abstract><div><p> and <abstract><p> variants.
    """
    abstract_elem = root.find(
        ".//tei:profileDesc/tei:abstract", NS
    )
    if abstract_elem is None:
        return []

    paragraphs = []

    # Try <div><p> first (most common)
    div_paragraphs = abstract_elem.findall(
        "tei:div/tei:p", NS
    )
    if div_paragraphs:
        for p_elem in div_paragraphs:
            paragraphs.append(_parse_paragraph(p_elem))
    else:
        # Try direct <p> elements
        for p_elem in abstract_elem.findall("tei:p", NS):
            paragraphs.append(_parse_paragraph(p_elem))

    return paragraphs


def _parse_keywords(root: etree._Element) -> list[str]:
    """Extract keywords from //textClass//keywords/term."""
    keywords: list[str] = []
    for term in root.findall(
        ".//tei:textClass//tei:keywords/tei:term", NS
    ):
        kwd_text = all_text(term)
        if kwd_text:
            keywords.append(kwd_text)
    return keywords


def _parse_doi(root: etree._Element) -> str:
    """Extract DOI from //sourceDesc//idno[@type='DOI']."""
    doi_elem = root.find(
        ".//tei:sourceDesc//tei:idno[@type='DOI']", NS
    )
    return text(doi_elem)


def _parse_body(root: etree._Element) -> list[Section]:
    """Parse body sections from //body/div."""
    sections: list[Section] = []
    body = root.find(".//tei:body", NS)
    if body is None:
        return sections

    for div in body.findall("tei:div", NS):
        section = _parse_section(div, level=1)
        sections.append(section)

    return sections


def _parse_top_level_figures(root: etree._Element) -> tuple[list[Figure], list[Table]]:
    """Parse figures/tables that are direct children of body (not in divs).

    GROBID commonly places extracted figures and tables at the end of
    <body> as siblings of <div> elements.
    """
    figures: list[Figure] = []
    tables: list[Table] = []
    body = root.find(".//tei:body", NS)
    if body is None:
        return figures, tables

    for child in body:
        tag = etree.QName(child.tag).localname if isinstance(
            child.tag, str
        ) else ""
        if tag == "figure":
            fig_type = child.get("type", "")
            if fig_type == "table":
                tables.append(_parse_table(child))
            else:
                figures.append(_parse_figure(child))

    return figures, tables


def _parse_section(div_elem: etree._Element, level: int) -> Section:
    """Recursively parse a div element into a Section."""
    section = Section(level=level)

    head = div_elem.find("tei:head", NS)
    if head is not None:
        section.heading = all_text(head)
        n_attr = head.get("n", "")
        section.number = n_attr

    for child in div_elem:
        tag = etree.QName(child.tag).localname if isinstance(
            child.tag, str
        ) else ""

        if tag == "head":
            continue  # already handled
        elif tag == "p":
            section.paragraphs.append(_parse_paragraph(child))
        elif tag == "div":
            subsection = _parse_section(child, level=level + 1)
            section.subsections.append(subsection)
        elif tag == "figure":
            fig_type = child.get("type", "")
            if fig_type == "table":
                section.tables.append(_parse_table(child))
            else:
                section.figures.append(_parse_figure(child))
        elif tag == "formula":
            section.formulas.append(_parse_formula(child))
        elif tag == "list":
            section.lists.append(_parse_list(child))
        elif tag == "note":
            place = child.get("place", "")
            if place == "foot":
                note_text = all_text(child)
                if note_text:
                    section.notes.append(note_text)

    return section


def _format_hi(elem: etree._Element) -> str:
    """Format a <hi> element with markdown syntax based on @rend."""
    rend = elem.get("rend", "")
    inner = all_text(elem)
    if not inner:
        return ""
    if rend == "italic":
        return f"*{inner}*"
    elif rend == "bold":
        return f"**{inner}**"
    elif rend == "superscript":
        return f"<sup>{inner}</sup>"
    elif rend == "subscript":
        return f"<sub>{inner}</sub>"
    return inner


def _parse_inline(elem: etree._Element, parts: list[str],
                  refs: list[InlineRef]) -> None:
    """Parse an inline child element, appending to parts and refs."""
    tag = etree.QName(elem.tag).localname if isinstance(
        elem.tag, str
    ) else ""

    if tag == "ref":
        ref_text = all_text(elem)
        ref_target = elem.get("target", "")
        if ref_text:
            refs.append(InlineRef(text=ref_text, target=ref_target))
            parts.append(ref_text)
    elif tag == "hi":
        parts.append(_format_hi(elem))
    else:
        parts.append(all_text(elem))


def _parse_paragraph(p_elem: etree._Element) -> Paragraph:
    """Parse a <p> element with mixed content (text + refs + tails)."""
    parts: list[str] = []
    refs: list[InlineRef] = []

    if p_elem.text:
        parts.append(p_elem.text)

    for child in p_elem:
        tag = etree.QName(child.tag).localname if isinstance(
            child.tag, str
        ) else ""

        if tag == "s":
            # Sentence segmentation — collect text content
            if child.text:
                parts.append(child.text)
            for grandchild in child:
                _parse_inline(grandchild, parts, refs)
                if grandchild.tail:
                    parts.append(grandchild.tail)
            if child.tail:
                parts.append(child.tail)
            continue
        else:
            _parse_inline(child, parts, refs)

        if child.tail:
            parts.append(child.tail)

    para_text = "".join(parts).strip()
    return Paragraph(text=para_text, refs=refs)


def _parse_figure(fig_elem: etree._Element) -> Figure:
    """Parse a <figure> element (non-table)."""
    fig = Figure()

    head = fig_elem.find("tei:head", NS)
    if head is not None:
        fig.label = all_text(head)

    figdesc = fig_elem.find("tei:figDesc", NS)
    if figdesc is not None:
        fig.caption = all_text(figdesc)

    graphic = fig_elem.find("tei:graphic", NS)
    if graphic is not None:
        fig.graphic_url = graphic.get("url", "")

    return fig


def _parse_table(fig_elem: etree._Element) -> Table:
    """Parse a <figure type='table'> element."""
    table = Table()

    head = fig_elem.find("tei:head", NS)
    if head is not None:
        table.label = all_text(head)

    figdesc = fig_elem.find("tei:figDesc", NS)
    if figdesc is not None:
        table.caption = all_text(figdesc)

    table_elem = fig_elem.find("tei:table", NS)
    if table_elem is not None:
        for row_elem in table_elem.findall("tei:row", NS):
            is_header_row = row_elem.get("role", "") == "head"
            row = []
            for cell_elem in row_elem.findall("tei:cell", NS):
                try:
                    colspan = int(cell_elem.get("cols", "1") or "1")
                except (ValueError, OverflowError):
                    colspan = 1
                cell = TableCell(
                    text=all_text(cell_elem),
                    is_header=is_header_row,
                )
                row.append(cell)
                for _ in range(colspan - 1):
                    row.append(TableCell(
                        text="", is_header=is_header_row,
                    ))
            if row:
                table.rows.append(row)

    # Table footnotes
    for note in fig_elem.findall("tei:note", NS):
        note_text = all_text(note)
        if note_text:
            if table.caption:
                table.caption += f" {note_text}"
            else:
                table.caption = note_text

    return table


def _parse_formula(formula_elem: etree._Element) -> Formula:
    """Parse a <formula> element."""
    # Get text content but exclude <label> text
    label_elem = formula_elem.find("tei:label", NS)
    label_text = all_text(label_elem)

    full_text = all_text(formula_elem)
    # Remove label from text — may appear at start or end
    if label_text:
        if full_text.endswith(label_text):
            formula_text = full_text[:-len(label_text)].strip()
        elif full_text.startswith(label_text):
            formula_text = full_text[len(label_text):].strip()
        else:
            formula_text = full_text
    else:
        formula_text = full_text

    return Formula(text=formula_text, label=label_text)


def _parse_list(list_elem: etree._Element) -> ListBlock:
    """Parse a <list> element."""
    list_type = list_elem.get("type", "")
    ordered = list_type in ("ordered", "order", "number")
    items: list[str] = []
    for item_elem in list_elem.findall("tei:item", NS):
        item_text = all_text(item_elem)
        if item_text:
            items.append(item_text)
    return ListBlock(items=items, ordered=ordered)


def _parse_acknowledgments(root: etree._Element) -> str:
    """Extract acknowledgments from //back/div[@type='acknowledgement']."""
    ack_div = root.find(
        ".//tei:back/tei:div[@type='acknowledgement']", NS
    )
    if ack_div is None:
        return ""

    parts: list[str] = []
    for p_elem in ack_div.findall(".//tei:p", NS):
        p_text = all_text(p_elem)
        if p_text:
            parts.append(p_text)
    return "\n\n".join(parts)


def _parse_annex(root: etree._Element) -> list[Section]:
    """Extract annex sections from //back/div[@type='annex']."""
    sections: list[Section] = []
    annex_div = root.find(
        ".//tei:back/tei:div[@type='annex']", NS
    )
    if annex_div is None:
        return sections

    for div in annex_div.findall("tei:div", NS):
        section = _parse_section(div, level=1)
        sections.append(section)

    return sections


def _parse_additional_back(root: etree._Element) -> list[Section]:
    """Extract additional back-matter sections (funding, availability, etc.).

    GROBID produces these as ``<div type="...">`` children of ``<back>``.
    """
    sections: list[Section] = []
    back = root.find(".//tei:back", NS)
    if back is None:
        return sections

    # Types already handled elsewhere
    _HANDLED_TYPES = {"acknowledgement", "annex"}

    for div in back.findall("tei:div", NS):
        div_type = div.get("type", "")
        if div_type in _HANDLED_TYPES:
            continue

        section = Section(level=1)

        # Use the div type as heading if no explicit head
        head = div.find("tei:head", NS)
        if head is not None:
            section.heading = all_text(head)
        elif div_type:
            # Capitalize the type for a readable heading
            section.heading = div_type.replace("_", " ").title()

        # Parse content — may have nested divs
        for child in div:
            tag = etree.QName(child.tag).localname if isinstance(
                child.tag, str
            ) else ""
            if tag == "head":
                continue
            elif tag == "p":
                section.paragraphs.append(_parse_paragraph(child))
            elif tag == "div":
                sub = _parse_section(child, level=2)
                section.subsections.append(sub)

        if (section.paragraphs or section.subsections
                or section.heading):
            sections.append(section)

    return sections


def _parse_bibliography(root: etree._Element) -> list[Reference]:
    """Extract references from //back//listBibl/biblStruct."""
    references = []
    for idx, bib in enumerate(
        root.findall(".//tei:back//tei:listBibl/tei:biblStruct", NS)
    ):
        ref = _parse_bib_entry(bib, idx + 1)
        references.append(ref)
    return references


def _parse_bib_authors(
    bib_elem: etree._Element, ref: Reference,
) -> None:
    """Extract authors from a ``<biblStruct>`` into *ref*."""
    for author_elem in bib_elem.findall(
        ".//tei:author/tei:persName", NS
    ):
        forenames = author_elem.findall("tei:forename", NS)
        given_parts = [text(fn) for fn in forenames if text(fn)]
        given = " ".join(given_parts)
        surname = text(author_elem.find("tei:surname", NS))
        if surname:
            name = f"{surname} {given}" if given else surname
            ref.authors.append(name)


def _parse_bib_imprint(
    bib_elem: etree._Element, ref: Reference,
) -> None:
    """Extract volume, issue, pages, year from imprint."""
    for scope in bib_elem.findall(
        ".//tei:monogr/tei:imprint/tei:biblScope", NS
    ):
        unit = scope.get("unit", "")
        if unit == "volume":
            ref.volume = text(scope)
        elif unit == "issue":
            ref.issue = text(scope)
        elif unit == "page":
            from_page = scope.get("from", "")
            to_page = scope.get("to", "")
            if from_page and to_page:
                ref.pages = f"{from_page}-{to_page}"
            elif from_page:
                ref.pages = from_page
            else:
                ref.pages = text(scope)

    date_elem = bib_elem.find(
        ".//tei:monogr/tei:imprint/tei:date", NS
    )
    if date_elem is not None:
        ref.year = date_elem.get("when", "")
        if len(ref.year) > 4:
            ref.year = ref.year[:4]


def _parse_bib_ids(
    bib_elem: etree._Element, ref: Reference,
) -> None:
    """Extract identifiers and external links from bibliography entry."""
    for idno_type, attr in (("DOI", "doi"), ("PMID", "pmid")):
        elem = bib_elem.find(f"tei:idno[@type='{idno_type}']", NS)
        if elem is not None:
            setattr(ref, attr, text(elem))

    for ptr_elem in bib_elem.findall(".//tei:ptr", NS):
        target = ptr_elem.get("target", "")
        if target:
            ref.ext_links.append(target)


def _parse_bib_entry(bib_elem: etree._Element, index: int) -> Reference:
    """Parse a single <biblStruct> element."""
    ref = Reference(index=index)

    _parse_bib_authors(bib_elem, ref)

    # Title — article-level first
    title_elem = bib_elem.find(
        ".//tei:analytic/tei:title[@level='a']", NS
    )
    if title_elem is not None:
        ref.title = all_text(title_elem)

    # Journal — fall back to monograph title for books/proceedings
    journal_elem = bib_elem.find(
        ".//tei:monogr/tei:title[@level='j']", NS
    )
    if journal_elem is not None:
        ref.journal = all_text(journal_elem)
    if not ref.journal:
        monogr_title = bib_elem.find(
            ".//tei:monogr/tei:title[@level='m']", NS
        )
        if monogr_title is not None:
            ref.journal = all_text(monogr_title)

    _parse_bib_imprint(bib_elem, ref)
    _parse_bib_ids(bib_elem, ref)

    return ref
