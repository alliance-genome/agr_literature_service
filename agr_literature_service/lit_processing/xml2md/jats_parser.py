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

    # Back matter: appendices + additional sections (fn-group, notes, etc.)
    back_matter = _parse_appendices(root)
    back_matter.extend(_parse_back_sections(root))
    doc.back_matter = back_matter

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

        # ORCID — try contrib-id first, then uri
        orcid_elem = contrib.find(
            "contrib-id[@contrib-id-type='orcid']"
        )
        if orcid_elem is not None:
            author.orcid = text(orcid_elem)

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


def _dispatch_sec_block(
    child: etree._Element, tag: str, section: Section,
) -> None:
    """Handle supplementary, boxed-text, quote, def-list, etc."""
    if tag == "supplementary-material":
        _parse_supplementary(child, section)
    elif tag == "boxed-text":
        _parse_boxed_text(child, section)
    elif tag == "disp-quote":
        quote_text = all_text(child)
        if quote_text:
            section.paragraphs.append(
                Paragraph(text=f"> {quote_text}")
            )
    elif tag == "def-list":
        _parse_def_list(child, section)
    elif tag == "fn-group":
        for fn in child.findall("fn"):
            fn_text = all_text(fn)
            if fn_text:
                section.notes.append(fn_text)
    elif tag == "preformat":
        pre_text = all_text(child)
        if pre_text:
            section.paragraphs.append(
                Paragraph(text=f"```\n{pre_text}\n```")
            )
    elif tag == "glossary":
        _parse_glossary(child, section)


_SEC_BLOCK_TAGS = frozenset({
    "supplementary-material", "boxed-text", "disp-quote",
    "def-list", "fn-group", "preformat", "glossary",
})


def _dispatch_sec_child(
    child: etree._Element, tag: str,
    section: Section, level: int,
) -> None:
    """Handle a single child element of a <sec>."""
    if tag in ("title", "label"):
        return
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
    elif tag in _SEC_BLOCK_TAGS:
        _dispatch_sec_block(child, tag, section)


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
        _dispatch_sec_child(child, tag, section, level)

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
        elif tag == "ext-link":
            link_text = all_text(child)
            href = child.get(
                "{http://www.w3.org/1999/xlink}href", ""
            )
            if not href:
                href = child.get("href", "")
            if link_text and href and link_text != href:
                parts.append(f"[{link_text}]({href})")
            elif href:
                parts.append(href)
            elif link_text:
                parts.append(link_text)
        elif tag == "italic":
            parts.append(f"*{all_text(child)}*")
        elif tag == "bold":
            parts.append(f"**{all_text(child)}**")
        elif tag == "sup":
            parts.append(f"<sup>{all_text(child)}</sup>")
        elif tag == "sub":
            parts.append(f"<sub>{all_text(child)}</sub>")
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

    # Table footnotes from <table-wrap-foot>
    foot = tw_elem.find("table-wrap-foot")
    if foot is not None:
        for fn in foot.findall(".//fn"):
            fn_text = all_text(fn)
            if fn_text:
                table.foot_notes.append(fn_text)

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
            try:
                colspan = int(child.get("colspan", "1") or "1")
            except (ValueError, OverflowError):
                colspan = 1
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


def _parse_supplementary(elem: etree._Element, section: Section) -> None:
    """Parse <supplementary-material> into a paragraph."""
    label_elem = elem.find("label")
    label = all_text(label_elem)
    caption = elem.find("caption")
    caption_text = ""
    if caption is not None:
        parts = []
        title = caption.find("title")
        if title is not None:
            parts.append(all_text(title))
        for p in caption.findall("p"):
            parts.append(all_text(p))
        caption_text = " ".join(parts)

    if label and caption_text:
        section.paragraphs.append(
            Paragraph(text=f"**{label}.** {caption_text}")
        )
    elif label:
        section.paragraphs.append(Paragraph(text=f"**{label}.**"))
    elif caption_text:
        section.paragraphs.append(Paragraph(text=caption_text))


def _parse_boxed_text(elem: etree._Element, section: Section) -> None:
    """Parse <boxed-text> — emit its content as regular paragraphs."""
    title_elem = elem.find("title")
    if title_elem is not None:
        title_text = all_text(title_elem)
        if title_text:
            section.paragraphs.append(
                Paragraph(text=f"**{title_text}**")
            )
    for p in elem.findall(".//p"):
        p_text = all_text(p)
        if p_text:
            section.paragraphs.append(Paragraph(text=p_text))


def _parse_def_list(elem: etree._Element, section: Section) -> None:
    """Parse <def-list> into a list block."""
    items: list[str] = []
    for def_item in elem.findall("def-item"):
        term_elem = def_item.find("term")
        def_elem = def_item.find("def")
        term = all_text(term_elem) if term_elem is not None else ""
        defn = all_text(def_elem) if def_elem is not None else ""
        if term and defn:
            items.append(f"**{term}**: {defn}")
        elif term:
            items.append(f"**{term}**")
        elif defn:
            items.append(defn)
    if items:
        section.lists.append(ListBlock(items=items, ordered=False))


def _parse_glossary(elem: etree._Element, section: Section) -> None:
    """Parse <glossary> — title + def-list or paragraphs."""
    title_elem = elem.find("title")
    if title_elem is not None:
        title_text = all_text(title_elem)
        if title_text:
            section.paragraphs.append(
                Paragraph(text=f"**{title_text}**")
            )
    for dl in elem.findall("def-list"):
        _parse_def_list(dl, section)
    for p in elem.findall("p"):
        p_text = all_text(p)
        if p_text:
            section.paragraphs.append(Paragraph(text=p_text))


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
        # Parse sub-sections within appendix
        for sec in app.findall("sec"):
            section.subsections.append(_parse_sec(sec, level=2))
        for p in app.findall("p"):
            section.paragraphs.append(_parse_paragraph(p))
        for tw in app.findall("table-wrap"):
            section.tables.append(_parse_table_wrap(tw))
        for fig in app.findall("fig"):
            section.figures.append(_parse_fig(fig))
        sections.append(section)

    return sections


def _parse_back_sections(root: etree._Element) -> list[Section]:
    """Extract additional back-matter sections (fn-group, notes, sec).

    These appear as direct children of <back> alongside <ack>,
    <ref-list>, and <app-group>.  They include footnotes, author
    contributions, data-availability statements, COI disclosures, etc.
    """
    sections: list[Section] = []
    back = root.find(".//back")
    if back is None:
        return sections

    for child in back:
        tag = etree.QName(child.tag).localname if isinstance(
            child.tag, str
        ) else ""

        if tag == "sec":
            sections.append(_parse_sec(child, level=1))
        elif tag == "fn-group":
            section = Section(level=1)
            title_elem = child.find("title")
            if title_elem is not None:
                section.heading = all_text(title_elem)
            for fn in child.findall("fn"):
                fn_text = all_text(fn)
                if fn_text:
                    section.notes.append(fn_text)
            if section.notes or section.heading:
                sections.append(section)
        elif tag == "notes":
            section = Section(level=1)
            title_elem = child.find("title")
            if title_elem is not None:
                section.heading = all_text(title_elem)
            for p in child.findall(".//p"):
                p_text = all_text(p)
                if p_text:
                    section.paragraphs.append(Paragraph(text=p_text))
            if section.paragraphs or section.heading:
                sections.append(section)
        elif tag == "glossary":
            section = Section(level=1)
            title_elem = child.find("title")
            if title_elem is not None:
                section.heading = all_text(title_elem)
            else:
                section.heading = "Glossary"
            _parse_glossary(child, section)
            if (section.paragraphs or section.lists
                    or section.heading):
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


def _find_citation(ref_elem: etree._Element) -> etree._Element | None:
    """Locate the citation element within a <ref>.

    Checks direct children and ``<citation-alternatives>`` wrapper.
    """
    for tag in ("element-citation", "mixed-citation"):
        elem = ref_elem.find(tag)
        if elem is not None:
            return elem
        elem = ref_elem.find(f"citation-alternatives/{tag}")
        if elem is not None:
            return elem
    return None


def _parse_ref_authors(
    citation: etree._Element, ref: Reference,
) -> None:
    """Extract authors from a citation element into *ref*."""
    # Try person-group/name first, fall back to direct name children
    author_names = citation.findall(".//person-group/name")
    if not author_names:
        author_names = citation.findall("name")
    for name_elem in author_names:
        surname = text(name_elem.find("surname"))
        given = text(name_elem.find("given-names"))
        if surname:
            name = f"{surname} {given}" if given else surname
            ref.authors.append(name)

    # Fall back to string-name (alternate format in some nXML)
    if not ref.authors:
        for sn in citation.findall(".//string-name"):
            surname = text(sn.find("surname"))
            given = text(sn.find("given-names"))
            if surname:
                name = f"{surname} {given}" if given else surname
                ref.authors.append(name)
            else:
                sn_text = all_text(sn)
                if sn_text:
                    ref.authors.append(sn_text)

    # Group/collaborative authors
    for collab_elem in citation.findall(".//collab"):
        collab_text = all_text(collab_elem)
        if collab_text:
            ref.authors.append(collab_text)


def _parse_ref_editors(
    citation: etree._Element, ref: Reference,
) -> None:
    """Extract editors from a citation element into *ref*."""
    editor_group = citation.find(
        "person-group[@person-group-type='editor']"
    )
    if editor_group is None:
        return
    for name_elem in editor_group.findall("name"):
        surname = text(name_elem.find("surname"))
        given = text(name_elem.find("given-names"))
        if surname:
            name = f"{surname} {given}" if given else surname
            ref.editors.append(name)


def _parse_ref_pages(
    citation: etree._Element, ref: Reference,
) -> None:
    """Extract page range or elocation-id from a citation."""
    fpage = citation.find("fpage")
    lpage = citation.find("lpage")
    if fpage is not None:
        fpage_text = text(fpage)
        lpage_text = text(lpage) if lpage is not None else ""
        if fpage_text and lpage_text:
            ref.pages = f"{fpage_text}-{lpage_text}"
        elif fpage_text:
            ref.pages = fpage_text
    if not ref.pages:
        page_range = citation.find("page-range")
        if page_range is not None:
            ref.pages = text(page_range)
    if not ref.pages:
        eloc = citation.find("elocation-id")
        if eloc is not None:
            ref.pages = text(eloc)


def _parse_ref_ids(
    citation: etree._Element, ref: Reference,
) -> None:
    """Extract identifiers and external links from a citation."""
    for pub_id_type, attr in (
        ("doi", "doi"), ("pmid", "pmid"), ("pmcid", "pmcid"),
    ):
        elem = citation.find(f"pub-id[@pub-id-type='{pub_id_type}']")
        if elem is not None:
            setattr(ref, attr, text(elem))

    for ext_link in citation.findall("ext-link"):
        href = ext_link.get(
            "{http://www.w3.org/1999/xlink}href", ""
        )
        if not href:
            href = ext_link.get("href", "")
        if href:
            ref.ext_links.append(href)


def _parse_ref(ref_elem: etree._Element, index: int) -> Reference:
    """Parse a single <ref> element.

    Supports both <element-citation> and <mixed-citation>,
    including ``<citation-alternatives>`` wrappers.
    """
    ref = Reference(index=index)

    citation = _find_citation(ref_elem)
    if citation is None:
        return ref

    _parse_ref_authors(citation, ref)

    # Title
    title_elem = citation.find("article-title")
    if title_elem is not None:
        ref.title = all_text(title_elem)

    # Journal / Source
    source_elem = citation.find("source")
    if source_elem is not None:
        ref.journal = all_text(source_elem)

    # Chapter / part title (book chapters)
    part_title = citation.find("chapter-title")
    if part_title is None:
        part_title = citation.find("part-title")
    if part_title is not None:
        ref.chapter_title = all_text(part_title)

    # Simple text fields
    for tag, attr in (("year", "year"), ("volume", "volume"),
                      ("issue", "issue")):
        elem = citation.find(tag)
        if elem is not None:
            setattr(ref, attr, text(elem))

    # Publisher info
    pub_name = citation.find("publisher-name")
    if pub_name is not None:
        ref.publisher = all_text(pub_name)
    pub_loc = citation.find("publisher-loc")
    if pub_loc is not None:
        ref.publisher_loc = all_text(pub_loc)

    # Conference name
    conf = citation.find("conf-name")
    if conf is not None:
        ref.conference = all_text(conf)

    # Editors
    _parse_ref_editors(citation, ref)

    _parse_ref_pages(citation, ref)
    _parse_ref_ids(citation, ref)

    return ref
