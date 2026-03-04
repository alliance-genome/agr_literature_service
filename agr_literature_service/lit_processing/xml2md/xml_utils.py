"""Shared XML text-extraction utilities for JATS and TEI parsers."""
from __future__ import annotations

from lxml import etree


def parse_xml(xml_content: bytes) -> etree._Element:
    """Parse XML bytes into an element tree with safe defaults.

    Uses recover mode so malformed XML is handled gracefully, and
    disables network access, DTD loading, and entity resolution to
    prevent XXE attacks.
    """
    parser = etree.XMLParser(
        recover=True, no_network=True, load_dtd=False,
        resolve_entities=False, huge_tree=False,
    )
    root = etree.fromstring(xml_content, parser=parser)
    if root is None:
        raise ValueError("Could not parse XML content")
    return root


def text(elem: etree._Element | None) -> str:
    """Get stripped direct text content of an element, or empty string."""
    if elem is None or elem.text is None:
        return ""
    return elem.text.strip()


def all_text(elem: etree._Element | None) -> str:
    """Get all text content of an element including children (stripped).

    Uses lxml's built-in text serialisation instead of manual recursion
    to avoid stack overflow on deeply nested XML.
    """
    if elem is None:
        return ""
    return (etree.tostring(elem, method="text", encoding="unicode", with_tail=False) or "").strip()
