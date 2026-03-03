"""Orchestrator for XML-to-Markdown conversion.

Detects format (TEI or JATS) and dispatches to the appropriate parser,
then emits Markdown via the shared emitter.
"""
from __future__ import annotations

from lxml import etree

from agr_literature_service.lit_processing.xml2md.jats_parser import parse_jats
from agr_literature_service.lit_processing.xml2md.md_emitter import emit_markdown
from agr_literature_service.lit_processing.xml2md.tei_parser import parse_tei

TEI_NAMESPACE = "http://www.tei-c.org/ns/1.0"


def detect_format(xml_content: bytes) -> str:
    """Detect whether XML content is TEI or JATS.

    Args:
        xml_content: Raw bytes of an XML file.

    Returns:
        'tei' or 'jats'.

    Raises:
        ValueError: If format cannot be determined.
    """
    try:
        parser = etree.XMLParser(
            recover=True, no_network=True,
            load_dtd=False, resolve_entities=False,
        )
        root = etree.fromstring(xml_content, parser=parser)
    except etree.XMLSyntaxError as e:
        raise ValueError(f"Unknown format: invalid XML ({e})")

    if root is None:
        raise ValueError("Unknown format: could not parse XML")

    tag = root.tag
    # TEI: root is {namespace}TEI or just TEI with namespace
    if tag == f"{{{TEI_NAMESPACE}}}TEI" or tag == "TEI":
        return "tei"
    # JATS: root is <article>
    if etree.QName(tag).localname == "article":
        return "jats"

    raise ValueError(f"Unknown format: unrecognized root element <{tag}>")


def convert_xml_to_markdown(
    xml_content: bytes, source_format: str = "auto"
) -> str:
    """Convert TEI or JATS XML to a Markdown string.

    Args:
        xml_content: Raw bytes of an XML file.
        source_format: One of 'auto', 'tei', or 'jats'.
            If 'auto', format is detected from the XML content.

    Returns:
        A docling-style Markdown string.

    Raises:
        ValueError: If format is unknown or cannot be detected.
    """
    if source_format == "auto":
        source_format = detect_format(xml_content)

    if source_format == "tei":
        document = parse_tei(xml_content)
    elif source_format == "jats":
        document = parse_jats(xml_content)
    else:
        raise ValueError(f"Unknown format: {source_format}")

    return emit_markdown(document)
