"""XML-to-Markdown conversion module.

Converts GROBID TEI XML and PMC nXML/JATS files to docling-style Markdown.
"""

from agr_literature_service.lit_processing.xml2md.converter import (  # noqa: F401
    convert_xml_to_markdown,
    detect_format,
)
