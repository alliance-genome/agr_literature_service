# XML-to-Markdown API Endpoint Design

**Date:** 2026-03-03
**Status:** Approved

## Summary

Add a stateless `POST /xml2md/convert` endpoint that accepts an XML file upload (TEI or JATS/nXML) and returns the converted Markdown as plain text. Supports format autodetection or explicit format selection via query parameter.

## Endpoint Specification

**`POST /xml2md/convert`**

### Parameters

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `file` | UploadFile | Yes | — | XML file (multipart/form-data) |
| `source_format` | str (query) | No | `"auto"` | One of `"auto"`, `"tei"`, `"jats"` |

### Responses

| Status | Content-Type | Description |
|---|---|---|
| `200` | `text/plain` | Markdown conversion result |
| `400` | `application/json` | Invalid `source_format` value |
| `422` | `application/json` | Conversion failed (malformed XML, unrecognized format, empty content) |

### Authentication

Requires authentication via `Security(get_authenticated_user)`, consistent with all other endpoints.

## Implementation

### New file: `agr_literature_service/api/routers/xml2md_router.py`

- `APIRouter(prefix="/xml2md", tags=["XML to Markdown"])`
- Single `POST /convert` endpoint
- Reads uploaded file bytes
- Calls `convert_xml_to_markdown(xml_content, source_format)` from `agr_literature_service.lit_processing.xml2md`
- Returns `PlainTextResponse` with the Markdown string
- Catches `ValueError` from converter, returns HTTP 422

### Modified file: `agr_literature_service/api/main.py`

- Import `xml2md_router` from routers
- Register with `app.include_router(xml2md_router.router)`

### Dependencies

None new. Uses existing:
- `agr_literature_service.lit_processing.xml2md.convert_xml_to_markdown`
- FastAPI (`UploadFile`, `File`, `APIRouter`, `Query`)
- `starlette.responses.PlainTextResponse`

## Design Decisions

1. **Dedicated router** — The conversion is a standalone utility, not tied to referencefiles or any database entity.
2. **Plain text response** — Markdown is the only output; no need for JSON wrapping.
3. **Stateless** — No database, no S3, no side effects. Pure conversion.
4. **Auth required** — Consistent with all other endpoints in the service.
