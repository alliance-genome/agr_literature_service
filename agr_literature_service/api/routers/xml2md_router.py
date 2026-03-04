"""XML-to-Markdown conversion endpoint."""

import asyncio
import logging
from typing import Any, Dict, Literal, Optional

from fastapi import APIRouter, File, Query, Security, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, Response
from markdown_it import MarkdownIt
from starlette import status

from agr_literature_service.api.auth import get_authenticated_user
from agr_literature_service.lit_processing.xml2md import (
    convert_xml_to_markdown,
    validate_markdown,
)

logger = logging.getLogger(__name__)

MAX_UPLOAD_BYTES = 10 * 1024 * 1024  # 10 MB

# Module-level renderer is safe: MarkdownIt.render() creates a new StateCore
# per call, so no shared mutable state across concurrent requests.
# html_inline is disabled to prevent XSS — entity-encoded tags in XML source
# (e.g. &lt;script&gt;) would otherwise render as live HTML.  This means
# <sup>/<sub> from the parsers display as literal text in the HTML preview;
# the plain-Markdown output (the primary format) is unaffected.
_md_renderer = MarkdownIt().disable("html_block").disable("html_inline")

_HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<style>
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
         max-width: 900px; margin: 2em auto; padding: 0 1em; line-height: 1.6; color: #24292f; }
  table { border-collapse: collapse; margin: 1em 0; }
  th, td { border: 1px solid #d0d7de; padding: 6px 13px; }
  th { background: #f6f8fa; }
  pre { background: #f6f8fa; padding: 1em; overflow-x: auto; border-radius: 6px; }
  code { background: #f6f8fa; padding: 0.2em 0.4em; border-radius: 3px; font-size: 85%; }
  pre code { background: none; padding: 0; }
  blockquote { border-left: 4px solid #d0d7de; margin: 0; padding: 0 1em; color: #57606a; }
  h1 { border-bottom: 1px solid #d0d7de; padding-bottom: 0.3em; }
  h2 { border-bottom: 1px solid #d0d7de; padding-bottom: 0.3em; }
</style>
</head>
<body>
<!-- __XML2MD_BODY__ -->
</body>
</html>"""

_HTML_BODY_MARKER = "<!-- __XML2MD_BODY__ -->"

router = APIRouter(
    prefix="/xml2md",
    tags=["XML to Markdown"],
)


@router.post(
    "/convert",
    status_code=status.HTTP_200_OK,
    summary="Convert XML to Markdown",
    description=(
        "Convert a GROBID TEI XML or PMC nXML/JATS file to docling-style Markdown. "
        "Set source_format to 'auto' (default) for autodetection, "
        "or explicitly to 'tei' or 'jats'. "
        "Set output_format to 'html' to get rendered Markdown (useful for Swagger preview)."
    ),
)
async def convert_xml_to_md(
    file: UploadFile = File(..., description="XML file to convert (TEI or JATS/nXML)"),
    source_format: Literal["auto", "tei", "jats"] = Query(
        "auto",
        description="Source format: 'auto' (autodetect), 'tei', or 'jats'",
    ),
    output_format: Literal["md", "html"] = Query(
        "md",
        description="Output format: 'md' (plain Markdown text) or 'html' (rendered HTML)",
    ),
    _user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
) -> Response:
    """Convert an uploaded XML file to Markdown."""
    xml_content = await file.read(MAX_UPLOAD_BYTES + 1)
    if not xml_content:
        return PlainTextResponse(
            content="Uploaded file is empty",
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        )
    if len(xml_content) > MAX_UPLOAD_BYTES:
        return PlainTextResponse(
            content="File too large (max 10 MB)",
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
        )

    try:
        markdown = await asyncio.to_thread(
            convert_xml_to_markdown, xml_content, source_format
        )
    except ValueError as e:
        logger.warning("XML-to-Markdown conversion failed: %s", e)
        return PlainTextResponse(
            content="Conversion failed due to invalid or unsupported input.",
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        )
    except Exception:
        logger.exception("Unexpected error during XML-to-Markdown conversion")
        return PlainTextResponse(
            content="An internal error occurred. Check server logs for details.",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )

    if output_format == "html":
        html_body = _md_renderer.render(markdown)
        html_page = _HTML_TEMPLATE.replace(_HTML_BODY_MARKER, html_body)
        return HTMLResponse(content=html_page)

    return PlainTextResponse(content=markdown)


@router.post(
    "/validate",
    status_code=status.HTTP_200_OK,
    summary="Validate Markdown against AGR schema",
    description=(
        "Upload a Markdown file and validate it against the AGR Literature "
        "Markdown Schema. Returns a JSON report with any errors and warnings."
    ),
)
async def validate_md(
    file: UploadFile = File(
        ..., description="Markdown file to validate",
    ),
    _user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
) -> Response:
    """Validate an uploaded Markdown file against the schema."""
    content = await file.read(MAX_UPLOAD_BYTES + 1)
    if not content:
        return PlainTextResponse(
            content="Uploaded file is empty",
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        )
    if len(content) > MAX_UPLOAD_BYTES:
        return PlainTextResponse(
            content="File too large (max 10 MB)",
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
        )

    text = content.decode("utf-8", errors="replace")
    result = await asyncio.to_thread(validate_markdown, text)

    errors = [
        {"rule_id": i.rule_id, "severity": i.severity.value,
         "line": i.line, "message": i.message}
        for i in result.errors
    ]
    warnings = [
        {"rule_id": i.rule_id, "severity": i.severity.value,
         "line": i.line, "message": i.message}
        for i in result.warnings
    ]

    return JSONResponse(content={
        "valid": result.valid,
        "error_count": len(errors),
        "warning_count": len(warnings),
        "errors": errors,
        "warnings": warnings,
    })
