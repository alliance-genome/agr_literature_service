"""Intermediate document model for XML-to-Markdown conversion."""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Author:
    given_name: str = ""
    surname: str = ""
    email: str = ""
    orcid: str = ""
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
    graphic_url: str = ""  # image ref (preserved for downstream; not in Markdown)


@dataclass
class TableCell:
    text: str = ""
    is_header: bool = False


@dataclass
class Table:
    label: str = ""              # "Table 1"
    caption: str = ""
    foot_notes: list[str] = field(default_factory=list)
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
    subsections: list[Section] = field(default_factory=list)


@dataclass
class Reference:
    """A single entry in the bibliography."""
    index: int = 0
    authors: list[str] = field(default_factory=list)  # "Surname FN" format
    editors: list[str] = field(default_factory=list)
    title: str = ""
    chapter_title: str = ""  # part-title for book chapters
    journal: str = ""
    publisher: str = ""
    publisher_loc: str = ""
    conference: str = ""  # conf-name / meeting
    volume: str = ""
    issue: str = ""
    pages: str = ""
    year: str = ""
    doi: str = ""
    pmid: str = ""
    pmcid: str = ""
    ext_links: list[str] = field(default_factory=list)  # URLs from ext-link/ptr


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
