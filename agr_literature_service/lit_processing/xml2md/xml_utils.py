"""Shared XML text-extraction utilities for JATS and TEI parsers."""
from __future__ import annotations


def text(elem) -> str:
    """Get stripped direct text content of an element, or empty string."""
    if elem is None or elem.text is None:
        return ""
    return elem.text.strip()


def all_text(elem) -> str:
    """Get all text content of an element including children (stripped)."""
    return collect_text(elem).strip()


def collect_text(elem) -> str:
    """Recursively collect text without stripping inner whitespace."""
    if elem is None:
        return ""
    parts: list[str] = []
    if elem.text:
        parts.append(elem.text)
    for child in elem:
        parts.append(collect_text(child))
        if child.tail:
            parts.append(child.tail)
    return "".join(parts)
