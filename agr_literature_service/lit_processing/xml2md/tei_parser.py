"""Parse GROBID TEI XML into the intermediate Document model."""
from __future__ import annotations

from lxml import etree

from agr_literature_service.lit_processing.xml2md.models import (
    Author, Document, Figure, Formula, InlineRef, ListBlock,
    Paragraph, Reference, Section, Table, TableCell,
)

NS = {"tei": "http://www.tei-c.org/ns/1.0"}


def parse_tei(xml_content: bytes) -> Document:
    """Parse GROBID TEI XML content into a Document model.

    Args:
        xml_content: Raw bytes of a TEI XML file.

    Returns:
        A populated Document dataclass.
    """
    root = etree.fromstring(xml_content)
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
    doc.back_matter = _parse_annex(root)

    return doc


def _text(elem) -> str:
    """Get stripped text content of an element, or empty string."""
    if elem is None or elem.text is None:
        return ""
    return elem.text.strip()


def _all_text(elem) -> str:
    """Get all text content of an element including children."""
    if elem is None:
        return ""
    parts = []
    if elem.text:
        parts.append(elem.text)
    for child in elem:
        parts.append(_all_text(child))
        if child.tail:
            parts.append(child.tail)
    return "".join(parts).strip()


def _parse_title(root) -> str:
    """Extract title from //teiHeader//title[@level='a']."""
    title_elem = root.find(".//tei:teiHeader//tei:title[@level='a']", NS)
    return _all_text(title_elem)


def _parse_authors(root) -> list:
    """Extract authors from //sourceDesc//author."""
    authors = []
    for author_elem in root.findall(
        ".//tei:sourceDesc//tei:author", NS
    ):
        author = Author()
        persname = author_elem.find("tei:persName", NS)
        if persname is not None:
            forename = persname.find("tei:forename", NS)
            surname = persname.find("tei:surname", NS)
            author.given_name = _text(forename)
            author.surname = _text(surname)

        email_elem = author_elem.find(".//tei:email", NS)
        if email_elem is not None:
            author.email = _text(email_elem)

        for aff_elem in author_elem.findall("tei:affiliation", NS):
            parts = []
            for orgname in aff_elem.findall("tei:orgName", NS):
                text = _text(orgname)
                if text:
                    parts.append(text)
            address = aff_elem.find("tei:address", NS)
            if address is not None:
                country = address.find("tei:country", NS)
                if country is not None and _text(country):
                    parts.append(_text(country))
            if parts:
                author.affiliations.append(", ".join(parts))

        authors.append(author)
    return authors


def _parse_abstract(root) -> list:
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


def _parse_keywords(root) -> list:
    """Extract keywords from //textClass//keywords/term."""
    keywords = []
    for term in root.findall(
        ".//tei:textClass//tei:keywords/tei:term", NS
    ):
        text = _all_text(term)
        if text:
            keywords.append(text)
    return keywords


def _parse_doi(root) -> str:
    """Extract DOI from //sourceDesc//idno[@type='DOI']."""
    doi_elem = root.find(
        ".//tei:sourceDesc//tei:idno[@type='DOI']", NS
    )
    return _text(doi_elem)


def _parse_body(root) -> list:
    """Parse body sections from //body/div."""
    sections: list[Section] = []
    body = root.find(".//tei:body", NS)
    if body is None:
        return sections

    for div in body.findall("tei:div", NS):
        section = _parse_section(div, level=1)
        sections.append(section)

    return sections


def _parse_top_level_figures(root) -> tuple[list, list]:
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


def _parse_section(div_elem, level: int) -> Section:
    """Recursively parse a div element into a Section."""
    section = Section(level=level)

    head = div_elem.find("tei:head", NS)
    if head is not None:
        section.heading = _all_text(head)
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
                note_text = _all_text(child)
                if note_text:
                    section.notes.append(note_text)

    return section


def _parse_paragraph(p_elem) -> Paragraph:
    """Parse a <p> element with mixed content (text + refs + tails)."""
    parts = []
    refs = []

    if p_elem.text:
        parts.append(p_elem.text)

    for child in p_elem:
        tag = etree.QName(child.tag).localname if isinstance(
            child.tag, str
        ) else ""

        if tag == "ref":
            ref_text = _all_text(child)
            ref_target = child.get("target", "")
            if ref_text:
                refs.append(InlineRef(text=ref_text, target=ref_target))
                parts.append(ref_text)
        elif tag == "s":
            # Sentence segmentation — collect text content
            if child.text:
                parts.append(child.text)
            for grandchild in child:
                gc_tag = etree.QName(grandchild.tag).localname if isinstance(
                    grandchild.tag, str
                ) else ""
                if gc_tag == "ref":
                    ref_text = _all_text(grandchild)
                    ref_target = grandchild.get("target", "")
                    if ref_text:
                        refs.append(InlineRef(
                            text=ref_text, target=ref_target
                        ))
                        parts.append(ref_text)
                else:
                    parts.append(_all_text(grandchild))
                if grandchild.tail:
                    parts.append(grandchild.tail)
            if child.tail:
                parts.append(child.tail)
            continue
        else:
            parts.append(_all_text(child))

        if child.tail:
            parts.append(child.tail)

    text = "".join(parts).strip()
    return Paragraph(text=text, refs=refs)


def _parse_figure(fig_elem) -> Figure:
    """Parse a <figure> element (non-table)."""
    fig = Figure()

    head = fig_elem.find("tei:head", NS)
    if head is not None:
        fig.label = _all_text(head)

    figdesc = fig_elem.find("tei:figDesc", NS)
    if figdesc is not None:
        fig.caption = _all_text(figdesc)

    graphic = fig_elem.find("tei:graphic", NS)
    if graphic is not None:
        fig.graphic_url = graphic.get("url", "")

    return fig


def _parse_table(fig_elem) -> Table:
    """Parse a <figure type='table'> element."""
    table = Table()

    head = fig_elem.find("tei:head", NS)
    if head is not None:
        table.label = _all_text(head)

    figdesc = fig_elem.find("tei:figDesc", NS)
    if figdesc is not None:
        table.caption = _all_text(figdesc)

    table_elem = fig_elem.find("tei:table", NS)
    if table_elem is not None:
        for row_elem in table_elem.findall("tei:row", NS):
            row = []
            for cell_elem in row_elem.findall("tei:cell", NS):
                cell = TableCell(text=_all_text(cell_elem))
                row.append(cell)
            if row:
                table.rows.append(row)

    return table


def _parse_formula(formula_elem) -> Formula:
    """Parse a <formula> element."""
    # Get text content but exclude <label> text
    label_elem = formula_elem.find("tei:label", NS)
    label_text = _all_text(label_elem)

    full_text = _all_text(formula_elem)
    # Remove label from the full text
    if label_text and full_text.endswith(label_text):
        formula_text = full_text[:-len(label_text)].strip()
    else:
        formula_text = full_text

    return Formula(text=formula_text, label=label_text)


def _parse_list(list_elem) -> ListBlock:
    """Parse a <list> element."""
    items = []
    for item_elem in list_elem.findall("tei:item", NS):
        text = _all_text(item_elem)
        if text:
            items.append(text)
    return ListBlock(items=items, ordered=False)


def _parse_acknowledgments(root) -> str:
    """Extract acknowledgments from //back/div[@type='acknowledgement']."""
    ack_div = root.find(
        ".//tei:back/tei:div[@type='acknowledgement']", NS
    )
    if ack_div is None:
        return ""

    parts = []
    for p_elem in ack_div.findall(".//tei:p", NS):
        text = _all_text(p_elem)
        if text:
            parts.append(text)
    return "\n\n".join(parts)


def _parse_annex(root) -> list:
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


def _parse_bibliography(root) -> list:
    """Extract references from //back//listBibl/biblStruct."""
    references = []
    for idx, bib in enumerate(
        root.findall(".//tei:back//tei:listBibl/tei:biblStruct", NS)
    ):
        ref = _parse_bib_entry(bib, idx + 1)
        references.append(ref)
    return references


def _parse_bib_entry(bib_elem, index: int) -> Reference:
    """Parse a single <biblStruct> element."""
    ref = Reference(index=index)

    # Authors
    for author_elem in bib_elem.findall(
        ".//tei:author/tei:persName", NS
    ):
        forename = _text(author_elem.find("tei:forename", NS))
        surname = _text(author_elem.find("tei:surname", NS))
        if surname:
            if forename:
                ref.authors.append(f"{surname} {forename}")
            else:
                ref.authors.append(surname)

    # Title
    title_elem = bib_elem.find(
        ".//tei:analytic/tei:title[@level='a']", NS
    )
    if title_elem is not None:
        ref.title = _all_text(title_elem)

    # Journal
    journal_elem = bib_elem.find(
        ".//tei:monogr/tei:title[@level='j']", NS
    )
    if journal_elem is not None:
        ref.journal = _all_text(journal_elem)

    # Volume, issue, pages, year
    for scope in bib_elem.findall(
        ".//tei:monogr/tei:imprint/tei:biblScope", NS
    ):
        unit = scope.get("unit", "")
        if unit == "volume":
            ref.volume = _text(scope)
        elif unit == "issue":
            ref.issue = _text(scope)
        elif unit == "page":
            from_page = scope.get("from", "")
            to_page = scope.get("to", "")
            if from_page and to_page:
                ref.pages = f"{from_page}-{to_page}"
            elif from_page:
                ref.pages = from_page
            else:
                ref.pages = _text(scope)

    date_elem = bib_elem.find(
        ".//tei:monogr/tei:imprint/tei:date", NS
    )
    if date_elem is not None:
        ref.year = date_elem.get("when", "")
        # Sometimes year is in full date format like "2024-01-01"
        if len(ref.year) > 4:
            ref.year = ref.year[:4]

    # DOI
    doi_elem = bib_elem.find("tei:idno[@type='DOI']", NS)
    if doi_elem is not None:
        ref.doi = _text(doi_elem)

    return ref
