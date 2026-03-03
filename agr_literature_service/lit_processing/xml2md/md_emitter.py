"""Emit docling-style Markdown from the intermediate Document model."""
from __future__ import annotations

from agr_literature_service.lit_processing.xml2md.models import (
    Document, Section,
)

MAX_HEADING_LEVEL = 6


def emit_markdown(doc: Document) -> str:
    """Convert a Document model to a Markdown string.

    Args:
        doc: A populated Document dataclass.

    Returns:
        A docling-style Markdown string.
    """
    lines: list[str] = []

    _emit_title(doc, lines)
    _emit_authors(doc, lines)
    _emit_abstract(doc, lines)
    _emit_keywords(doc, lines)
    _emit_sections(doc.sections, lines, base_level=2)
    _emit_doc_level_figures(doc, lines)
    _emit_doc_level_tables(doc, lines)
    _emit_acknowledgments(doc, lines)
    _emit_back_matter(doc, lines)
    _emit_references(doc, lines)

    return "\n".join(lines).rstrip("\n") + "\n"


def _emit_title(doc: Document, lines: list[str]) -> None:
    if not doc.title:
        return
    lines.append(f"# {doc.title}")
    lines.append("")


def _emit_authors(doc: Document, lines: list[str]) -> None:
    if not doc.authors:
        return

    # Collect unique affiliations and assign superscript numbers
    aff_map: dict[str, int] = {}
    for author in doc.authors:
        for aff in author.affiliations:
            if aff not in aff_map:
                aff_map[aff] = len(aff_map) + 1

    # Build author line
    author_parts = []
    for author in doc.authors:
        name = f"{author.given_name} {author.surname}".strip()
        if author.affiliations and aff_map:
            sups = ",".join(
                str(aff_map[a]) for a in author.affiliations if a in aff_map
            )
            name = f"{name}^{sups}"
        author_parts.append(name)

    lines.append(f"Authors: {', '.join(author_parts)}")
    lines.append("")

    # Affiliation footnotes
    for aff, num in aff_map.items():
        lines.append(f"^{num} {aff}")
    if aff_map:
        lines.append("")


def _emit_abstract(doc: Document, lines: list[str]) -> None:
    if not doc.abstract:
        return
    lines.append("## Abstract")
    lines.append("")
    for para in doc.abstract:
        lines.append(para.text)
        lines.append("")


def _emit_keywords(doc: Document, lines: list[str]) -> None:
    if not doc.keywords:
        return
    lines.append(f"**Keywords:** {', '.join(doc.keywords)}")
    lines.append("")


def _emit_sections(
    sections: list[Section], lines: list[str], base_level: int
) -> None:
    for section in sections:
        _emit_section(section, lines, base_level)


def _emit_section(
    section: Section, lines: list[str], heading_level: int
) -> None:
    capped_level = min(heading_level, MAX_HEADING_LEVEL)
    hashes = "#" * capped_level

    # Heading
    if section.heading:
        if section.number:
            lines.append(f"{hashes} {section.number} {section.heading}")
        else:
            lines.append(f"{hashes} {section.heading}")
        lines.append("")

    # Paragraphs
    for para in section.paragraphs:
        lines.append(para.text)
        lines.append("")

    # Figures
    for fig in section.figures:
        label = fig.label.rstrip(".:").strip()
        if fig.caption:
            lines.append(f"**{label}.** {fig.caption}")
        else:
            lines.append(f"**{label}.**")
        lines.append("")

    # Tables
    for table in section.tables:
        _emit_table(table, lines)

    # Formulas
    for formula in section.formulas:
        lines.append(f"> Formula: {formula.text}")
        lines.append("")

    # Lists
    for lst in section.lists:
        _emit_list(lst, lines)

    # Footnotes
    for i, note in enumerate(section.notes, 1):
        lines.append(f"[^{i}]: {note}")
    if section.notes:
        lines.append("")

    # Subsections
    for sub in section.subsections:
        _emit_section(sub, lines, heading_level + 1)


def _emit_table(table, lines: list[str]) -> None:
    if not table.rows:
        return

    # Determine column count from widest row
    col_count = max(len(row) for row in table.rows)

    # Header row (first row)
    header = table.rows[0]
    header_cells = [cell.text for cell in header]
    # Pad if needed
    while len(header_cells) < col_count:
        header_cells.append("")
    lines.append("| " + " | ".join(header_cells) + " |")
    lines.append("|" + "|".join(["---"] * col_count) + "|")

    # Data rows
    for row in table.rows[1:]:
        cells = [cell.text for cell in row]
        while len(cells) < col_count:
            cells.append("")
        lines.append("| " + " | ".join(cells) + " |")
    lines.append("")

    # Caption
    if table.label or table.caption:
        label = table.label.rstrip(".:").strip()
        if table.caption:
            lines.append(f"**{label}.** {table.caption}")
        else:
            lines.append(f"**{label}.**")
        lines.append("")


def _emit_list(lst, lines: list[str]) -> None:
    for i, item in enumerate(lst.items, 1):
        if lst.ordered:
            lines.append(f"{i}. {item}")
        else:
            lines.append(f"- {item}")
    lines.append("")


def _emit_doc_level_figures(doc: Document, lines: list[str]) -> None:
    """Emit figures that are at document level (not inside sections)."""
    for fig in doc.figures:
        label = fig.label.rstrip(".:").strip()
        if label:
            if fig.caption:
                lines.append(f"**{label}.** {fig.caption}")
            else:
                lines.append(f"**{label}.**")
        elif fig.caption:
            lines.append(fig.caption)
        else:
            continue
        lines.append("")


def _emit_doc_level_tables(doc: Document, lines: list[str]) -> None:
    """Emit tables that are at document level (not inside sections)."""
    for table in doc.tables:
        _emit_table(table, lines)


def _emit_acknowledgments(doc: Document, lines: list[str]) -> None:
    if not doc.acknowledgments:
        return
    lines.append("## Acknowledgments")
    lines.append("")
    lines.append(doc.acknowledgments)
    lines.append("")


def _emit_back_matter(doc: Document, lines: list[str]) -> None:
    if not doc.back_matter:
        return
    _emit_sections(doc.back_matter, lines, base_level=2)


def _emit_references(doc: Document, lines: list[str]) -> None:
    if not doc.references:
        return
    lines.append("## References")
    lines.append("")
    for ref in doc.references:
        parts = []
        # Authors
        if ref.authors:
            parts.append(", ".join(ref.authors))
        # Year
        if ref.year:
            parts.append(f"({ref.year})")
        # Title
        if ref.title:
            parts.append(f"{ref.title}.")
        # Journal (italic)
        if ref.journal:
            journal_part = f"*{ref.journal}*"
            if ref.volume:
                journal_part += f", {ref.volume}"
                if ref.issue:
                    journal_part += f"({ref.issue})"
            if ref.pages:
                journal_part += f", {ref.pages}"
            journal_part += "."
            parts.append(journal_part)
        # DOI
        if ref.doi:
            parts.append(f"doi:{ref.doi}")

        line = f"{ref.index}. " + " ".join(parts)
        lines.append(line)
    lines.append("")
